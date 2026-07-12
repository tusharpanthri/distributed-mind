"""Dask engine — LocalCluster with S3 storage via s3fs wired to MinIO."""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urlparse

import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client, LocalCluster
from minio import Minio
from minio.deleteobjects import DeleteObject

from benchmark.metrics import RunMetrics, measure
from engines.base import BenchmarkEngine, BenchmarkResult

logger = logging.getLogger("distributedmind.dask")


class DaskEngine(BenchmarkEngine):
    """Runs the benchmark transformation using Dask with a LocalCluster."""

    def __init__(self) -> None:
        super().__init__()
        self._cluster: LocalCluster | None = None
        self._client: Client | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def setup(self, config: dict[str, Any]) -> None:
        self._config = config
        dask_cfg = config.get("dask", {})

        self._cluster = LocalCluster(
            n_workers=dask_cfg.get("n_workers", 2),
            threads_per_worker=dask_cfg.get("threads_per_worker", 2),
            memory_limit=dask_cfg.get("memory_limit", "2GB"),
        )
        self._client = Client(self._cluster)
        logger.info("Dask LocalCluster created", extra={"dashboard": self._client.dashboard_link})

    def teardown(self) -> None:
        if self._client:
            self._client.close()
            self._client = None
        if self._cluster:
            self._cluster.close()
            self._cluster = None
            logger.info("Dask cluster stopped")

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
                engine_name="dask",
                duration_seconds=metrics.duration_seconds,
                rows_processed=rows_processed,
                rows_output=rows_output,
                peak_memory_mb=metrics.peak_memory_mb,
                success=True,
            )
        except Exception as exc:
            logger.exception("Dask run failed")
            return BenchmarkResult(
                engine_name="dask",
                duration_seconds=metrics.duration_seconds,
                rows_processed=0,
                rows_output=0,
                peak_memory_mb=metrics.peak_memory_mb,
                success=False,
                error_message=str(exc),
            )

    def _clear_output_prefix(self, output_path: str) -> None:
        """Delete existing objects under output_path via the MinIO SDK.

        s3fs's async bulk-delete (used by dask's to_parquet overwrite=True)
        fails against this MinIO release with a MissingContentMD5 error, so
        the prefix is cleared here with the synchronous minio client instead.
        """
        minio_cfg = self._config["minio"]
        parsed = urlparse(output_path)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")

        client = Minio(
            minio_cfg["endpoint"].replace("http://", "").replace("https://", ""),
            access_key=minio_cfg["access_key"],
            secret_key=minio_cfg["secret_key"],
            secure=minio_cfg["endpoint"].startswith("https"),
        )
        if not client.bucket_exists(bucket):
            return
        objects = client.list_objects(bucket, prefix=prefix, recursive=True)
        delete_objects = (DeleteObject(obj.object_name) for obj in objects)
        for error in client.remove_objects(bucket, delete_objects):
            logger.warning("Failed to delete object", extra={"error": str(error)})

    def _transform(self, input_path: str, output_path: str, lookup_path: str) -> tuple[int, int]:
        minio = self._config["minio"]
        storage_options = {
            "key": minio["access_key"],
            "secret": minio["secret_key"],
            "endpoint_url": minio["endpoint"],
        }

        # Convert s3a:// → s3:// (s3fs uses s3://)
        s3_input = input_path.replace("s3a://", "s3://")
        s3_output = output_path.replace("s3a://", "s3://")
        s3_lookup = lookup_path.replace("s3a://", "s3://")

        # 1. Read partitioned Parquet from MinIO
        events = dd.read_parquet(s3_input, storage_options=storage_options)

        # 2. Filter to PushEvent only
        events = events[
            (events["type"] == "PushEvent") & events["repo_id"].notnull()
        ]
        rows_processed = int(len(events))

        # 3. Group by repo_id / repo_name, compute aggregates.
        # "nunique" isn't accepted inside groupby().agg() on this dask-expr
        # version, so it's computed as its own dedicated groupby call and merged.
        aggregated = (
            events.groupby(["repo_id", "repo_name"])
            .agg(
                event_count=("type", "count"),
                avg_payload_size=("payload_size", "mean"),
            )
            .reset_index()
        )
        unique_actors = (
            events.groupby(["repo_id", "repo_name"])["actor_login"]
            .nunique()
            .rename("unique_actors")
            .reset_index()
        )
        aggregated = aggregated.merge(unique_actors, on=["repo_id", "repo_name"], how="left")

        # 4. Join with repo-metadata lookup (left join → unknown for missing)
        lookup_pd: pd.DataFrame = dd.read_parquet(s3_lookup, storage_options=storage_options).compute()
        enriched = aggregated.merge(lookup_pd, on="repo_id", how="left")
        enriched["language"] = enriched["language"].fillna("unknown")
        enriched["repo_owner_type"] = enriched["repo_owner_type"].fillna("unknown")

        # 5. Write partitioned by language.
        # Prefix is cleared explicitly first (see _clear_output_prefix); the
        # write itself uses overwrite=False so dask never triggers s3fs's
        # bulk-delete path.
        self._clear_output_prefix(s3_output)
        enriched.to_parquet(
            s3_output,
            partition_on=["language"],
            storage_options=storage_options,
            write_index=False,
            overwrite=False,
        )

        rows_output = int(len(enriched))
        return rows_processed, rows_output
