"""PySpark engine — local mode with S3A connector wired to MinIO."""

from __future__ import annotations

import logging
import sys
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from benchmark.metrics import RunMetrics, measure
from engines.base import BenchmarkEngine, BenchmarkResult

logger = logging.getLogger("distributedmind.spark")


class SparkEngine(BenchmarkEngine):
    """Runs the benchmark transformation using PySpark in local mode."""

    def __init__(self) -> None:
        super().__init__()
        self._spark: SparkSession | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def setup(self, config: dict[str, Any]) -> None:
        self._config = config
        minio = config["minio"]
        spark_cfg = config.get("spark", {})
        endpoint = minio["endpoint"]

        self._spark = (
            SparkSession.builder.appName(spark_cfg.get("app_name", "DistributedMind"))
            .master(spark_cfg.get("master", "local[*]"))
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.access.key", minio["access_key"])
            .config("spark.hadoop.fs.s3a.secret.key", minio["secret_key"])
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.jars.packages",
                    "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        )
        self._spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created")

    def teardown(self) -> None:
        if self._spark:
            self._spark.stop()
            self._spark = None
            logger.info("Spark session stopped")

    # ------------------------------------------------------------------
    # Transformation
    # ------------------------------------------------------------------

    def run(self, input_path: str, output_path: str) -> BenchmarkResult:
        assert self._spark is not None, "Call setup() before run()"
        metrics = RunMetrics()

        try:
            with measure(metrics):
                lookup_path = self._config["benchmark"]["lookup_path"]
                rows_processed, rows_output = self._transform(input_path, output_path, lookup_path)

            return BenchmarkResult(
                engine_name="spark",
                duration_seconds=metrics.duration_seconds,
                rows_processed=rows_processed,
                rows_output=rows_output,
                peak_memory_mb=metrics.peak_memory_mb,
                success=True,
            )
        except Exception as exc:
            logger.exception("Spark run failed")
            return BenchmarkResult(
                engine_name="spark",
                duration_seconds=metrics.duration_seconds,
                rows_processed=0,
                rows_output=0,
                peak_memory_mb=metrics.peak_memory_mb,
                success=False,
                error_message=str(exc),
            )

    def _transform(self, input_path: str, output_path: str, lookup_path: str) -> tuple[int, int]:
        spark = self._spark
        assert spark is not None

        # 1. Read partitioned Parquet from MinIO
        events = spark.read.parquet(input_path)

        # 2. Filter to PushEvent only
        events = events.filter(
            (F.col("type") == "PushEvent")
            & (F.col("repo_id").isNotNull())
        )
        rows_processed = events.count()

        # 3. Group by repo_id / repo_name, compute aggregates
        aggregated = events.groupBy("repo_id", "repo_name").agg(
            F.count("*").alias("event_count"),
            F.countDistinct("actor_login").alias("unique_actors"),
            F.avg("payload_size").alias("avg_payload_size"),
        )

        # 4. Join with repo-metadata lookup (left join → unknown for missing)
        lookup = spark.read.parquet(lookup_path)
        enriched = aggregated.join(lookup, on="repo_id", how="left").fillna(
            {"language": "unknown", "repo_owner_type": "unknown"}
        )

        # 5. Write partitioned by language
        enriched.write.mode("overwrite").partitionBy("language").parquet(output_path)

        rows_output = enriched.count()
        return rows_processed, rows_output
