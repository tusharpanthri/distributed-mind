"""Ray Data engine — reads Parquet from MinIO via pyarrow S3 filesystem."""

from __future__ import annotations

import logging
from functools import partial
from typing import Any

import pandas as pd
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import ray
import ray.data

from engines.base import BenchmarkEngine, raise_injected_failure

logger = logging.getLogger("distributedmind.ray")


class RayEngine(BenchmarkEngine):
    """Runs the benchmark transformation using Ray Data."""

    name = "ray"

    def __init__(self) -> None:
        super().__init__()
        self._fs: pafs.S3FileSystem | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def setup(self, config: dict[str, Any]) -> None:
        self._config = config
        ray_cfg = config.get("ray", {})
        minio = config["minio"]

        if not ray.is_initialized():
            ray.init(
                num_cpus=ray_cfg.get("num_cpus", 4),
                object_store_memory=ray_cfg.get("object_store_memory", 1_073_741_824),
                include_dashboard=False,
                ignore_reinit_error=True,
            )

        endpoint = minio["endpoint"].replace("http://", "").replace("https://", "")
        self._fs = pafs.S3FileSystem(
            access_key=minio["access_key"],
            secret_key=minio["secret_key"],
            endpoint_override=endpoint,
            scheme="http",
        )
        logger.info("Ray initialized")

    def teardown(self) -> None:
        if ray.is_initialized():
            ray.shutdown()
            logger.info("Ray shutdown")
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

        # Strip s3a:// → bucket + path for pyarrow S3
        def _s3_path(uri: str) -> str:
            return uri.replace("s3a://", "").replace("s3://", "")

        # 1. Read partitioned Parquet from MinIO via Ray Data
        dataset = ray.data.read_parquet(
            f"s3://{_s3_path(input_path)}",
            filesystem=self._fs,
            columns=["id", "type", "actor_login", "repo_id", "repo_name", "payload_size"],
        )

        # 2. Filter to PushEvent only
        dataset = dataset.map_batches(_filter_push_events, batch_format="pandas")
        if inject_failure:
            dataset = dataset.map_batches(_failing_batch, batch_format="pandas")
        # Filtered events are materialized once, then counted and aggregated
        # (Spark caches and Dask persists at the same point).
        dataset = dataset.materialize()
        rows_processed = dataset.count()

        # 3. Group by repo_id / repo_name, compute aggregates.
        # Ray Data 2.20's built-in groupby aggregations iterate groups in Python
        # (~240s for 120k repos), so rows are hash-partitioned by key into a few
        # buckets instead: the shuffle is a cheap low-cardinality groupby on the
        # bucket id, and each bucket is aggregated with vectorized pandas.
        num_buckets = 2 * int(self._config.get("ray", {}).get("num_cpus", 4))
        if mitigate_skew:
            aggregated = self._aggregate_block_partitioned(dataset, num_buckets)
        else:
            aggregated = (
                dataset.map_batches(partial(_assign_bucket, num_buckets=num_buckets), batch_format="pandas")
                .groupby("bucket")
                .map_groups(_aggregate_bucket, batch_format="pandas")
                .to_pandas()
            )

        # 4. Join with repo-metadata lookup (left join → unknown for missing)
        lookup_table = pq.read_table(_s3_path(lookup_path), filesystem=self._fs)
        lookup_pd: pd.DataFrame = lookup_table.to_pandas()
        enriched = aggregated.merge(lookup_pd, on="repo_id", how="left")
        enriched["language"] = enriched["language"].fillna("unknown")
        enriched["repo_owner_type"] = enriched["repo_owner_type"].fillna("unknown")

        # 5. Write partitioned by language
        output_s3 = _s3_path(output_path)
        self._clear_prefix(output_s3)
        result_ds = ray.data.from_pandas(enriched)
        result_ds.write_parquet(
            f"s3://{output_s3}",
            filesystem=self._fs,
            partition_cols=["language"],
        )

        rows_output = len(enriched)
        return rows_processed, rows_output

    def _aggregate_block_partitioned(self, dataset: ray.data.Dataset, num_buckets: int) -> pd.DataFrame:
        """Custom block partitioning: hot repo_ids are spread over every bucket.

        Unmitigated, all rows of a hot repo hash to one bucket, so one task does
        a disproportionate share of the work. Here, every block is first
        pre-aggregated locally (map-side combine to one row per repo/actor),
        then partials of the top-N repos are spread across all buckets by
        hashing the *actor* — actor sets per bucket are disjoint, so per-bucket
        distinct counts still sum exactly — while long-tail repos keep hashing
        by repo. Buckets are aggregated in parallel and recombined.
        """
        top_n = int(self._config.get("skew", {}).get("hot_key_top_n", 10))

        partials = dataset.map_batches(_partial_aggregate, batch_format="pandas").materialize()
        counts = partials.map_batches(_repo_counts, batch_format="pandas").to_pandas()
        hot_ids = set(counts.groupby("repo_id")["cnt"].sum().nlargest(top_n).index.tolist())

        per_bucket = (
            partials.map_batches(partial(_assign_bucket, num_buckets=num_buckets, hot_ids=hot_ids),
                                 batch_format="pandas")
            .groupby("bucket")
            .map_groups(_aggregate_partials, batch_format="pandas")
            .to_pandas()
        )
        combined = per_bucket.groupby(["repo_id", "repo_name"], as_index=False).sum(numeric_only=True)
        combined["avg_payload_size"] = combined["psum"] / combined["pcnt"].where(combined["pcnt"] > 0)
        return combined[["repo_id", "repo_name", "event_count", "unique_actors", "avg_payload_size"]]

    def _clear_prefix(self, path: str) -> None:
        """Remove a previous run's output so re-runs don't accumulate files."""
        assert self._fs is not None
        info = self._fs.get_file_info(path)
        if info.type == pafs.FileType.Directory:
            self._fs.delete_dir_contents(path, missing_dir_ok=True)


# ---------------------------------------------------------------------------
# Batch / group functions (run on Ray workers)
# ---------------------------------------------------------------------------

def _filter_push_events(batch: pd.DataFrame) -> pd.DataFrame:
    return batch[(batch["type"] == "PushEvent") & batch["repo_id"].notna()]


def _failing_batch(batch: pd.DataFrame) -> pd.DataFrame:
    """Runs in a Ray task; the task errors and Ray Data aborts the dataset."""
    raise_injected_failure("ray")
    return batch


def _assign_bucket(batch: pd.DataFrame, num_buckets: int, hot_ids: set[int] | None = None) -> pd.DataFrame:
    """Hash rows to buckets by repo; rows of ``hot_ids`` repos hash by actor instead."""
    by_repo = pd.util.hash_pandas_object(batch[["repo_id", "repo_name"]], index=False).to_numpy()
    bucket = by_repo % num_buckets
    if hot_ids:
        by_actor = pd.util.hash_pandas_object(batch["actor_login"], index=False).to_numpy() % num_buckets
        hot = batch["repo_id"].isin(hot_ids).to_numpy()
        bucket[hot] = by_actor[hot]
    return batch.assign(bucket=bucket.astype("int64"))


def _aggregate_bucket(rows: pd.DataFrame) -> pd.DataFrame:
    return (
        rows.groupby(["repo_id", "repo_name"])
        .agg(event_count=("id", "size"), unique_actors=("actor_login", "nunique"),
             avg_payload_size=("payload_size", "mean"))
        .reset_index()
    )


def _partial_aggregate(batch: pd.DataFrame) -> pd.DataFrame:
    """Per-block partial aggregate: one row per (repo, actor) with its counts."""
    return (
        batch.groupby(["repo_id", "repo_name", "actor_login"], dropna=False)
        .agg(cnt=("id", "size"), psum=("payload_size", "sum"), pcnt=("payload_size", "count"))
        .reset_index()
    )


def _repo_counts(partials: pd.DataFrame) -> pd.DataFrame:
    return partials.groupby("repo_id", as_index=False)["cnt"].sum()


def _aggregate_partials(partials: pd.DataFrame) -> pd.DataFrame:
    """Combine one bucket's partials; distinct actors are exact within a bucket."""
    return (
        partials.groupby(["repo_id", "repo_name"])
        .agg(event_count=("cnt", "sum"), psum=("psum", "sum"), pcnt=("pcnt", "sum"),
             unique_actors=("actor_login", "nunique"))
        .reset_index()
    )
