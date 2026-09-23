"""Build an amplified-skew copy of the raw dataset in MinIO.

The top-N keys (``skew_key`` in the workload spec) have their rows replicated
``--skew-factor`` times, each copy getting a unique row id, so a handful of keys
dominate the dataset. Only aggregate counts grow: distinct counts over other
columns stay as they were. The original data stays in place as the ``balanced``
dataset; the copy is written to ``data.skewed_prefix`` as the ``skewed`` one.

Usage:
    python -m data.amplify_skew --top-n 5 --skew-factor 10
    python -m data.amplify_skew --workload config/workloads/actor-activity.yaml
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import click
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from benchmark.storage import s3_filesystem
from data.download_gharchive_data import _load_config, _setup_logging
from workloads.spec import WorkloadSpec, load_workload

logger = logging.getLogger("distributedmind.skew")


@dataclass
class SkewStats:
    """Distribution summary of rows per skew key."""

    total_events: int
    distinct_repos: int       # distinct skew-key values
    top_repo_share: float     # fraction of rows owned by the single largest key
    top_n_share: float        # fraction owned by the top-N keys
    max_to_median_ratio: float


def matching_rows(table: pa.Table, spec: WorkloadSpec) -> pa.Table:
    """Only the rows the workload aggregates, so skew is measured on those."""
    if spec.filter:
        table = table.filter(pc.equal(table.column(spec.filter.column), spec.filter.equals))
    for column in spec.require_not_null:
        table = table.filter(pc.is_valid(table.column(column)))
    return table


def top_keys(table: pa.Table, spec: WorkloadSpec, top_n: int) -> list[Any]:
    """The top-N skew keys by row count (ties broken by the key itself)."""
    key = spec.skew_key
    counts = table.group_by(key).aggregate([(key, "count")])
    counts = counts.sort_by([(f"{key}_count", "descending"), (key, "ascending")])
    return counts.column(key).to_pylist()[:top_n]


def skew_stats(table: pa.Table, spec: WorkloadSpec, top_n: int) -> SkewStats:
    key = spec.skew_key
    counts = table.group_by(key).aggregate([(key, "count")]).column(f"{key}_count")
    values = sorted(counts.to_pylist(), reverse=True)
    total = sum(values)
    median = values[len(values) // 2] if values else 0
    return SkewStats(
        total_events=total,
        distinct_repos=len(values),
        top_repo_share=values[0] / total if total else 0.0,
        top_n_share=sum(values[:top_n]) / total if total else 0.0,
        max_to_median_ratio=values[0] / median if median else 0.0,
    )


def amplify(table: pa.Table, hot_keys: list[Any], spec: WorkloadSpec, skew_factor: int) -> pa.Table:
    """Replicate rows of ``hot_keys`` so each appears ``skew_factor`` times in total."""
    if skew_factor < 1:
        raise ValueError("skew_factor must be >= 1")
    key_column = table.column(spec.skew_key)
    hot = pc.is_in(key_column, value_set=pa.array(hot_keys, key_column.type))
    hot_rows = table.filter(hot)
    row_id_index = table.schema.get_field_index(spec.row_id)

    copies = [table]
    for i in range(1, skew_factor):
        new_ids = pc.binary_join_element_wise(
            pc.cast(hot_rows.column(spec.row_id), pa.string()), pa.scalar(f"dup{i}"), "-"
        )
        copies.append(hot_rows.set_column(row_id_index, spec.row_id, new_ids))
    return pa.concat_tables(copies).cast(table.schema)


def run(config_path: str, top_n: int, skew_factor: int,
        workload_path: str | None = None) -> tuple[SkewStats, SkewStats]:
    cfg = _load_config(config_path)
    spec = load_workload(workload_path or cfg["benchmark"]["workload"])
    fs = s3_filesystem(cfg["minio"])
    bucket = cfg["minio"]["bucket_raw"]
    rows_per_file = int(cfg["data"].get("rows_per_file", 250_000)) or 250_000
    src = f"{bucket}/{cfg['data']['raw_prefix']}"
    dst = f"{bucket}/{cfg['data']['skewed_prefix']}"

    # hive partitioning surfaces the date=YYYY-MM-DD directory as a column so
    # the skewed copy keeps the same layout as the raw data.
    source = pq.read_table(src, filesystem=fs, partitioning="hive")
    dates = pc.unique(source.column("date").cast(pa.string())).to_pylist()
    table = source.drop_columns(["date"])
    spec.validate_against(table.schema, source=f"raw dataset ({src})")

    before = skew_stats(matching_rows(table, spec), spec, top_n)
    hot_keys = top_keys(matching_rows(table, spec), spec, top_n)
    fs.create_dir(dst, recursive=True)
    fs.delete_dir_contents(dst, missing_dir_ok=True)
    skewed_parts = []
    for date in dates:
        part = table.filter(pc.equal(source.column("date").cast(pa.string()), date))
        skewed_part = amplify(part, hot_keys, spec, skew_factor)
        for idx, offset in enumerate(range(0, skewed_part.num_rows, rows_per_file)):
            pq.write_table(skewed_part.slice(offset, rows_per_file),
                           f"{dst}/date={date}/events-{idx:04d}.parquet",
                           filesystem=fs, compression="snappy")
        skewed_parts.append(skewed_part)
    after = skew_stats(matching_rows(pa.concat_tables(skewed_parts), spec), spec, top_n)

    logger.info("Skew before", extra={"workload": spec.name, "top_n": top_n, **before.__dict__})
    logger.info("Skew after",
                extra={"workload": spec.name, "top_n": top_n, "skew_factor": skew_factor,
                       **after.__dict__})
    return before, after


@click.command()
@click.option("--top-n", default=None, type=int, help="Keys to amplify (default: config skew.amplify_top_n)")
@click.option("--skew-factor", default=None, type=int, help="Total copies of each hot row (default: config)")
@click.option("--config", "config_path", default="config/benchmark_config.yaml", show_default=True)
@click.option("--workload", "workload_path", default=None,
              help="Workload spec YAML (default: benchmark.workload from the config)")
def main(top_n: int | None, skew_factor: int | None, config_path: str,
         workload_path: str | None) -> None:
    """Write an amplified-skew copy of the raw dataset to MinIO."""
    _setup_logging()
    skew_cfg = _load_config(config_path).get("skew", {})
    top_n = top_n or int(skew_cfg.get("amplify_top_n", 5))
    skew_factor = skew_factor or int(skew_cfg.get("skew_factor", 10))
    before, after = run(config_path, top_n, skew_factor, workload_path)
    print(
        f"\nTop key share of rows:   {before.top_repo_share:.2%} -> {after.top_repo_share:.2%}"
        f"\nTop-{top_n} share:          {before.top_n_share:.2%} -> {after.top_n_share:.2%}"
        f"\nMax/median rows per key: {before.max_to_median_ratio:,.0f}x -> {after.max_to_median_ratio:,.0f}x"
        f"\nTotal rows:              {before.total_events:,} -> {after.total_events:,}\n"
    )


if __name__ == "__main__":
    main()
