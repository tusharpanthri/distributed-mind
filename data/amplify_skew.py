"""Build an amplified-skew copy of the raw GH Archive events in MinIO.

The top-N repos by PushEvent count have their rows replicated ``--skew-factor``
times (each copy gets a unique event id), so a handful of ``repo_id`` keys
dominate the dataset. The original data stays in place as the ``balanced``
dataset; the copy is written to ``data.skewed_prefix`` as the ``skewed`` one.

Usage:
    python -m data.amplify_skew --top-n 5 --skew-factor 10
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import click
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from data.download_gharchive_data import FLAT_SCHEMA, _load_config, _setup_logging

logger = logging.getLogger("distributedmind.skew")


@dataclass
class SkewStats:
    """Distribution summary of events per repo_id."""

    total_events: int
    distinct_repos: int
    top_repo_share: float     # fraction of all events owned by the single largest repo
    top_n_share: float        # fraction owned by the top-N repos
    max_to_median_ratio: float


def push_events(table: pa.Table) -> pa.Table:
    """The benchmark aggregates PushEvents only, so skew is measured on those."""
    return table.filter(pc.equal(table.column("type"), "PushEvent"))


def top_repo_ids(table: pa.Table, top_n: int) -> list[int]:
    """Return the top-N repo_ids by event count (ties broken by repo_id)."""
    counts = table.group_by("repo_id").aggregate([("repo_id", "count")])
    counts = counts.sort_by([("repo_id_count", "descending"), ("repo_id", "ascending")])
    return counts.column("repo_id").to_pylist()[:top_n]


def skew_stats(table: pa.Table, top_n: int) -> SkewStats:
    counts = table.group_by("repo_id").aggregate([("repo_id", "count")]).column("repo_id_count")
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


def amplify(table: pa.Table, hot_ids: list[int], skew_factor: int) -> pa.Table:
    """Replicate rows of ``hot_ids`` repos so each row appears ``skew_factor`` times in total."""
    if skew_factor < 1:
        raise ValueError("skew_factor must be >= 1")
    hot = pc.is_in(table.column("repo_id"), value_set=pa.array(hot_ids, pa.int64()))
    hot_rows = table.filter(hot)

    copies = [table]
    for i in range(1, skew_factor):
        new_ids = pc.binary_join_element_wise(hot_rows.column("id"), pa.scalar(f"dup{i}"), "-")
        copies.append(hot_rows.set_column(hot_rows.schema.get_field_index("id"), "id", new_ids))
    return pa.concat_tables(copies).cast(FLAT_SCHEMA)


def _filesystem(cfg: dict) -> pafs.S3FileSystem:
    minio = cfg["minio"]
    return pafs.S3FileSystem(
        access_key=minio["access_key"],
        secret_key=minio["secret_key"],
        endpoint_override=minio["endpoint"].replace("http://", "").replace("https://", ""),
        scheme="https" if minio["endpoint"].startswith("https") else "http",
    )


def run(config_path: str, top_n: int, skew_factor: int) -> tuple[SkewStats, SkewStats]:
    cfg = _load_config(config_path)
    fs = _filesystem(cfg)
    bucket = cfg["minio"]["bucket_raw"]
    rows_per_file = int(cfg["data"].get("rows_per_file", 250_000)) or 250_000
    src = f"{bucket}/{cfg['data']['raw_prefix']}"
    dst = f"{bucket}/{cfg['data']['skewed_prefix']}"

    # hive partitioning surfaces the date=YYYY-MM-DD directory as a column so
    # the skewed copy keeps the same layout as the raw data.
    source = pq.read_table(src, filesystem=fs, partitioning="hive")
    dates = pc.unique(source.column("date").cast(pa.string())).to_pylist()
    table = source.drop_columns(["date"]).cast(FLAT_SCHEMA)

    before = skew_stats(push_events(table), top_n)
    hot_ids = top_repo_ids(push_events(table), top_n)
    fs.create_dir(dst, recursive=True)
    fs.delete_dir_contents(dst, missing_dir_ok=True)
    skewed_parts = []
    for date in dates:
        part = table.filter(pc.equal(source.column("date").cast(pa.string()), date))
        skewed_part = amplify(part, hot_ids, skew_factor)
        for idx, offset in enumerate(range(0, skewed_part.num_rows, rows_per_file)):
            pq.write_table(skewed_part.slice(offset, rows_per_file),
                           f"{dst}/date={date}/events-{idx:04d}.parquet",
                           filesystem=fs, compression="snappy")
        skewed_parts.append(skewed_part)
    after = skew_stats(push_events(pa.concat_tables(skewed_parts)), top_n)

    logger.info("Skew before", extra={"top_n": top_n, **before.__dict__})
    logger.info("Skew after", extra={"top_n": top_n, "skew_factor": skew_factor, **after.__dict__})
    return before, after


@click.command()
@click.option("--top-n", default=None, type=int, help="Repos to amplify (default: config skew.amplify_top_n)")
@click.option("--skew-factor", default=None, type=int, help="Total copies of each hot row (default: config)")
@click.option("--config", "config_path", default="config/benchmark_config.yaml", show_default=True)
def main(top_n: int | None, skew_factor: int | None, config_path: str) -> None:
    """Write an amplified-skew copy of raw-data/events to MinIO."""
    _setup_logging()
    skew_cfg = _load_config(config_path).get("skew", {})
    top_n = top_n or int(skew_cfg.get("amplify_top_n", 5))
    skew_factor = skew_factor or int(skew_cfg.get("skew_factor", 10))
    before, after = run(config_path, top_n, skew_factor)
    print(
        f"\nTop repo share of events: {before.top_repo_share:.2%} -> {after.top_repo_share:.2%}"
        f"\nTop-{top_n} share:          {before.top_n_share:.2%} -> {after.top_n_share:.2%}"
        f"\nMax/median events per repo: {before.max_to_median_ratio:,.0f}x -> {after.max_to_median_ratio:,.0f}x"
        f"\nTotal events: {before.total_events:,} -> {after.total_events:,}\n"
    )


if __name__ == "__main__":
    main()
