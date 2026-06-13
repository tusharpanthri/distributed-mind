"""Ray Data engine — reads Parquet from MinIO via pyarrow S3 filesystem."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import ray
import ray.data

from benchmark.metrics import RunMetrics, measure
from engines.base import BenchmarkEngine, BenchmarkResult

logger = logging.getLogger("distributedmind.ray")


class RayEngine(BenchmarkEngine):
    """Runs the benchmark transformation using Ray Data."""

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

    def run(self, input_path: str, output_path: str) -> BenchmarkResult:
        metrics = RunMetrics()

        try:
            with measure(metrics):
                lookup_path = self._config["benchmark"]["lookup_path"]
                rows_processed, rows_output = self._transform(input_path, output_path, lookup_path)

            return BenchmarkResult(
                engine_name="ray",
                duration_seconds=metrics.duration_seconds,
                rows_processed=rows_processed,
                rows_output=rows_output,
                peak_memory_mb=metrics.peak_memory_mb,
                success=True,
            )
        except Exception as exc:
            logger.exception("Ray run failed")
            return BenchmarkResult(
                engine_name="ray",
                duration_seconds=metrics.duration_seconds,
                rows_processed=0,
                rows_output=0,
                peak_memory_mb=metrics.peak_memory_mb,
                success=False,
                error_message=str(exc),
            )

    def _transform(self, input_path: str, output_path: str, lookup_path: str) -> tuple[int, int]:
        assert self._fs is not None

        # Strip s3a:// → bucket + path for pyarrow S3
        def _s3_path(uri: str) -> str:
            return uri.replace("s3a://", "").replace("s3://", "")

        # 1. Read partitioned Parquet from MinIO via Ray Data
        dataset = ray.data.read_parquet(
            f"s3://{_s3_path(input_path)}",
            filesystem=self._fs,
        )

        # 2. Filter to PushEvent only
        dataset = dataset.filter(
            lambda row: row["type"] == "PushEvent" and row["repo_id"] is not None
        )
        rows_processed = dataset.count()

        # 3. Group by repo_id / repo_name, compute aggregates
        pd_events: pd.DataFrame = dataset.to_pandas()
        aggregated = (
            pd_events.groupby(["repo_id", "repo_name"])
            .agg(
                event_count=("type", "count"),
                unique_actors=("actor_login", "nunique"),
                avg_payload_size=("payload_size", "mean"),
            )
            .reset_index()
        )

        # 4. Join with repo-metadata lookup (left join → unknown for missing)
        lookup_table = pq.read_table(_s3_path(lookup_path), filesystem=self._fs)
        lookup_pd: pd.DataFrame = lookup_table.to_pandas()
        enriched = aggregated.merge(lookup_pd, on="repo_id", how="left")
        enriched["language"] = enriched["language"].fillna("unknown")
        enriched["repo_owner_type"] = enriched["repo_owner_type"].fillna("unknown")

        # 5. Write partitioned by language
        output_s3 = _s3_path(output_path)
        result_ds = ray.data.from_pandas(enriched)
        result_ds.write_parquet(
            f"s3://{output_s3}",
            filesystem=self._fs,
            partition_cols=["language"],
        )

        rows_output = len(enriched)
        return rows_processed, rows_output
