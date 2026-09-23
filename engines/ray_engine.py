"""Ray Data engine — reads Parquet from MinIO via pyarrow S3 filesystem."""

from __future__ import annotations

import logging
import subprocess
import time
from functools import partial
from typing import Any

import pandas as pd
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import ray
import ray.data

from engines.base import BenchmarkEngine, raise_injected_failure, workload_of
from workloads.spec import WorkloadSpec

logger = logging.getLogger("distributedmind.ray")


def _wait_for_cpus(timeout: float, poll: float = 2.0) -> None:
    """Block until worker nodes offer CPUs.

    The driver joins as a 0-CPU node, so without this a cluster whose workers
    haven't registered (or have died) would queue tasks forever instead of
    failing.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        cpus = ray.cluster_resources().get("CPU", 0)
        if cpus >= 1:
            logger.info("Ray cluster ready", extra={"cpus": cpus})
            return
        time.sleep(poll)
    raise RuntimeError(f"no Ray worker CPUs registered within {timeout:.0f}s")


class RayEngine(BenchmarkEngine):
    """Runs the benchmark transformation using Ray Data."""

    name = "ray"

    def __init__(self) -> None:
        super().__init__()
        self._fs: pafs.S3FileSystem | None = None
        self._joined_cluster = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def setup(self, config: dict[str, Any]) -> None:
        self._config = config
        ray_cfg = config.get("ray", {})
        minio = config["minio"]

        address = ray_cfg.get("address") or ""
        if not ray.is_initialized():
            if address:
                # Ray Data needs a raylet on the driver's own node ("Global node
                # is not initialized" under Ray Client), so the driver joins the
                # cluster as a 0-CPU node and connects to it locally. All compute
                # still happens on the ray-worker nodes.
                started = subprocess.run(
                    ["ray", "start", "--address", address, "--num-cpus=0", "--disable-usage-stats"],
                    capture_output=True, text=True, timeout=180,
                )
                if started.returncode != 0:
                    raise RuntimeError(f"ray start failed: {started.stderr.strip()[-500:]}")
                self._joined_cluster = True
                ray.init(address="auto", ignore_reinit_error=True)
                _wait_for_cpus(timeout=120)
            else:
                ray.init(
                    num_cpus=ray_cfg.get("num_cpus", 4),
                    object_store_memory=ray_cfg.get("object_store_memory", 1_073_741_824),
                    include_dashboard=False,
                    ignore_reinit_error=True,
                )

        endpoint = minio["endpoint"]
        self._fs = pafs.S3FileSystem(
            access_key=minio["access_key"],
            secret_key=minio["secret_key"],
            endpoint_override=endpoint.replace("http://", "").replace("https://", ""),
            scheme="https" if endpoint.startswith("https") else "http",
        )
        logger.info("Ray initialized", extra={"address": address or "local"})

    def teardown(self) -> None:
        if ray.is_initialized():
            ray.shutdown()
            logger.info("Ray shutdown")
        if self._joined_cluster:
            # Detach this driver node; the head and workers stay up.
            subprocess.run(["ray", "stop"], check=False, capture_output=True, timeout=120)
            self._joined_cluster = False
        self._fs = None

    # ------------------------------------------------------------------
    # Transformation
    # ------------------------------------------------------------------

    def _transform(
        self,
        input_path: str,
        output_path: str,
        lookup_path: str,
        mitigate_skew: bool,
        inject_failure: bool,
    ) -> tuple[int, int]:
        assert self._fs is not None, "Call setup() before run()"
        spec = workload_of(self._config)

        def _s3_path(uri: str) -> str:
            return uri.replace("s3a://", "").replace("s3://", "")

        # 1. Read only the columns this workload needs
        dataset = ray.data.read_parquet(
            f"s3://{_s3_path(input_path)}",
            filesystem=self._fs,
            columns=spec.input_columns,
        )

        # 2. Filter per the spec
        dataset = dataset.map_batches(partial(_filter_rows, spec=spec), batch_format="pandas")
        if inject_failure:
            dataset = dataset.map_batches(_failing_batch, batch_format="pandas")
        # Filtered events are materialized once, then counted and aggregated
        # (Spark caches and Dask persists at the same point).
        dataset = dataset.materialize()
        rows_processed = dataset.count()

        # 3. Aggregate per the spec.
        # Ray Data 2.20's built-in groupby aggregations iterate groups in Python
        # (~240s for 120k keys), so rows are hash-partitioned by key into a few
        # buckets instead: the shuffle is a cheap low-cardinality groupby on the
        # bucket id, and each bucket is aggregated with vectorized pandas.
        num_buckets = 2 * self._available_cpus()
        if mitigate_skew:
            aggregated = self._aggregate_block_partitioned(dataset, spec, num_buckets)
        else:
            aggregated = (
                dataset.map_batches(
                    partial(_assign_bucket, num_buckets=num_buckets, key_columns=spec.group_by),
                    batch_format="pandas",
                )
                .groupby("bucket")
                .map_groups(partial(_aggregate_bucket, spec=spec), batch_format="pandas")
                .to_pandas()
            )

        # 4. Join the lookup table (left join -> spec's `missing` for absent keys)
        if spec.join:
            lookup_table = pq.read_table(_s3_path(spec.join.path), filesystem=self._fs,
                                         columns=[spec.join.key, *spec.join.columns])
            lookup_pd: pd.DataFrame = lookup_table.to_pandas()
            aggregated = aggregated.merge(lookup_pd, on=spec.join.key, how="left")
            for column in spec.join.columns:
                aggregated[column] = aggregated[column].fillna(spec.join.missing)
        enriched = aggregated[spec.output_columns]

        # 5. Write, partitioned as the spec asks
        output_s3 = _s3_path(output_path)
        self._clear_prefix(output_s3)
        result_ds = ray.data.from_pandas(enriched)
        result_ds.write_parquet(
            f"s3://{output_s3}",
            filesystem=self._fs,
            partition_cols=[spec.partition_by] if spec.partition_by else None,
        )

        rows_output = len(enriched)
        return rows_processed, rows_output

    def _aggregate_block_partitioned(
        self, dataset: ray.data.Dataset, spec: WorkloadSpec, num_buckets: int
    ) -> pd.DataFrame:
        """Custom block partitioning: hot keys are spread over every bucket.

        Unmitigated, all rows of a hot key hash to one bucket, so one task does
        a disproportionate share of the work. Here, every block is first
        pre-aggregated locally (map-side combine to one row per group key plus
        distinct column), then partials of the top-N keys are spread across all
        buckets by hashing the *distinct* column - its value sets per bucket are
        disjoint, so per-bucket distinct counts still sum exactly - while
        long-tail keys keep hashing by group key. Buckets are aggregated in
        parallel and recombined.
        """
        distinct_columns = [a.column for a in spec.distinct_aggregations]
        if len(distinct_columns) > 1:
            raise ValueError(
                "Ray skew mitigation supports at most one distinct_count aggregation "
                f"(workload {spec.name!r} has {len(distinct_columns)}): spreading a hot key "
                "across buckets relies on one column's value sets being disjoint per bucket. "
                "Run this workload without --mitigation on, or use Spark/Dask."
            )
        top_n = int(self._config.get("skew", {}).get("hot_key_top_n", 10))

        partials = dataset.map_batches(
            partial(_partial_aggregate, spec=spec), batch_format="pandas"
        ).materialize()
        counts = partials.map_batches(
            partial(_key_counts, spec=spec), batch_format="pandas"
        ).to_pandas()
        hot_keys = set(
            counts.groupby(spec.skew_key)["__rows"].sum().nlargest(top_n).index.tolist()
        )

        per_bucket = (
            partials.map_batches(
                partial(_assign_bucket, num_buckets=num_buckets, key_columns=spec.group_by,
                        hot_keys=hot_keys, skew_key=spec.skew_key,
                        spread_column=distinct_columns[0] if distinct_columns else spec.row_id),
                batch_format="pandas",
            )
            .groupby("bucket")
            .map_groups(partial(_aggregate_partials, spec=spec), batch_format="pandas")
            .to_pandas()
        )
        return _combine_buckets(per_bucket, spec)

    def _available_cpus(self) -> int:
        """CPUs the cluster actually has (local mode: the configured count)."""
        if self._config.get("ray", {}).get("address"):
            return max(1, int(ray.cluster_resources().get("CPU", 1)))
        return int(self._config.get("ray", {}).get("num_cpus", 4))

    def _clear_prefix(self, path: str) -> None:
        """Remove a previous run's output so re-runs don't accumulate files."""
        assert self._fs is not None
        info = self._fs.get_file_info(path)
        if info.type == pafs.FileType.Directory:
            self._fs.delete_dir_contents(path, missing_dir_ok=True)


# ---------------------------------------------------------------------------
# Batch / group functions (run on Ray workers)
# ---------------------------------------------------------------------------

def _filter_rows(batch: pd.DataFrame, spec: WorkloadSpec) -> pd.DataFrame:
    if spec.filter:
        batch = batch[batch[spec.filter.column] == spec.filter.equals]
    for column in spec.require_not_null:
        batch = batch[batch[column].notna()]
    return batch


def _failing_batch(batch: pd.DataFrame) -> pd.DataFrame:
    """Runs in a Ray task; the task errors and Ray Data aborts the dataset."""
    raise_injected_failure("ray")
    return batch


def _assign_bucket(
    batch: pd.DataFrame,
    num_buckets: int,
    key_columns: list[str],
    hot_keys: set[Any] | None = None,
    skew_key: str | None = None,
    spread_column: str | None = None,
) -> pd.DataFrame:
    """Hash rows to buckets by group key; hot keys hash by ``spread_column``."""
    bucket = pd.util.hash_pandas_object(batch[key_columns], index=False).to_numpy() % num_buckets
    if hot_keys and skew_key and spread_column:
        spread = pd.util.hash_pandas_object(batch[spread_column], index=False).to_numpy() % num_buckets
        hot = batch[skew_key].isin(hot_keys).to_numpy()
        bucket[hot] = spread[hot]
    return batch.assign(bucket=bucket.astype("int64"))


def _pandas_aggs(spec: WorkloadSpec) -> dict[str, tuple[str, str]]:
    """Spec aggregations as pandas named-aggregation tuples."""
    named: dict[str, tuple[str, str]] = {}
    for aggregation in spec.aggregations:
        if aggregation.op == "count":
            named[aggregation.name] = (spec.row_id, "size")
        elif aggregation.op == "distinct_count":
            named[aggregation.name] = (aggregation.value_column, "nunique")
        else:
            named[aggregation.name] = (aggregation.value_column, aggregation.op)
    return named


def _aggregate_bucket(rows: pd.DataFrame, spec: WorkloadSpec) -> pd.DataFrame:
    return rows.groupby(spec.group_by).agg(**_pandas_aggs(spec)).reset_index()


def _partial_aggregate(batch: pd.DataFrame, spec: WorkloadSpec) -> pd.DataFrame:
    """Per-block partial aggregate: one row per group key (plus distinct column)."""
    keys = [*spec.group_by, *(a.column for a in spec.distinct_aggregations)]
    named: dict[str, tuple[str, str]] = {"__rows": (spec.row_id, "size")}
    for aggregation in spec.additive_aggregations:
        for field in aggregation.partial_fields():
            named[field.name] = (
                (spec.row_id, "size") if field.column is None else (field.value_column, field.op)
            )
    return batch.groupby(keys, dropna=False).agg(**named).reset_index()


def _key_counts(partials: pd.DataFrame, spec: WorkloadSpec) -> pd.DataFrame:
    return partials.groupby(spec.skew_key, as_index=False)["__rows"].sum()


def _aggregate_partials(partials: pd.DataFrame, spec: WorkloadSpec) -> pd.DataFrame:
    """Combine one bucket's partials; distinct counts are exact within a bucket."""
    named: dict[str, tuple[str, str]] = {}
    for aggregation in spec.additive_aggregations:
        for field in aggregation.partial_fields():
            named[field.name] = (field.name, "sum" if field.op == "count" else field.op)
    for aggregation in spec.distinct_aggregations:
        named[aggregation.name] = (aggregation.value_column, "nunique")
    return partials.groupby(spec.group_by).agg(**named).reset_index()


def _combine_buckets(per_bucket: pd.DataFrame, spec: WorkloadSpec) -> pd.DataFrame:
    """Fold per-bucket results into one row per group key."""
    named: dict[str, tuple[str, str]] = {}
    for aggregation in spec.additive_aggregations:
        for field in aggregation.partial_fields():
            named[field.name] = (field.name, "sum" if field.op == "count" else field.op)
    for aggregation in spec.distinct_aggregations:
        # Hot keys were spread so each bucket holds a disjoint value set.
        named[aggregation.name] = (aggregation.name, "sum")
    combined = per_bucket.groupby(spec.group_by).agg(**named).reset_index()

    for aggregation in spec.additive_aggregations:
        fields = aggregation.partial_fields()
        if aggregation.op == "mean":
            total, count = fields
            combined[aggregation.name] = combined[total.name] / combined[count.name].where(
                combined[count.name] > 0
            )
        else:
            combined[aggregation.name] = combined[fields[0].name]
    return combined[[*spec.group_by, *(a.name for a in spec.aggregations)]]
