"""PySpark engine — local mode with S3A connector wired to MinIO."""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

from engines.base import BenchmarkEngine, raise_injected_failure

logger = logging.getLogger("distributedmind.spark")


class SparkEngine(BenchmarkEngine):
    """Runs the benchmark transformation using PySpark in local mode."""

    name = "spark"

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

    def _transform(
        self,
        input_path: str,
        output_path: str,
        lookup_path: str,
        mitigate_skew: bool,
        inject_failure: bool,
    ) -> tuple[int, int]:
        spark = self._spark
        assert spark is not None, "Call setup() before run()"

        # 1. Read partitioned Parquet from MinIO
        events = spark.read.parquet(input_path)

        # 2. Filter to PushEvent only
        events = events.filter(
            (F.col("type") == "PushEvent")
            & (F.col("repo_id").isNotNull())
        )
        if inject_failure:
            events = events.filter(_failing_udf(F.col("repo_id")))
        # Filtered events are materialized once, then counted and aggregated
        # (Dask persists and Ray materializes at the same point).
        events = events.cache()
        rows_processed = events.count()

        # 3. Group by repo_id / repo_name, compute aggregates
        if mitigate_skew:
            aggregated = self._aggregate_salted(events)
        else:
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
        events.unpersist()
        return rows_processed, rows_output

    def _aggregate_salted(self, events: DataFrame) -> DataFrame:
        """Salting: split each hot repo_id into N sub-keys, aggregate, then recombine.

        Additive aggregates (count, sum, non-null count) are computed per
        (repo, salt) and summed. Distinct actors can't be summed across random
        salts, so they're computed by first de-duplicating (repo, actor) pairs:
        that shuffle is keyed on the actor too, so a hot repo's rows spread out,
        and map-side dedupe collapses bot accounts that push thousands of times.
        """
        skew_cfg = self._config.get("skew", {})
        top_n = int(skew_cfg.get("hot_key_top_n", 10))
        buckets = int(skew_cfg.get("salt_buckets", 8))

        hot_ids = [
            row["repo_id"]
            for row in events.groupBy("repo_id").count().orderBy(F.desc("count")).limit(top_n).collect()
        ]
        salt = F.when(F.col("repo_id").isin(hot_ids), F.pmod(F.hash("id"), F.lit(buckets))).otherwise(F.lit(0))

        partial = (
            events.withColumn("salt", salt)
            .groupBy("repo_id", "repo_name", "salt")
            .agg(
                F.count("*").alias("cnt"),
                F.sum("payload_size").alias("psum"),
                F.count("payload_size").alias("pcnt"),
            )
        )
        additive = partial.groupBy("repo_id", "repo_name").agg(
            F.sum("cnt").alias("event_count"),
            (F.sum("psum") / F.sum("pcnt")).alias("avg_payload_size"),
        )
        distinct_actors = (
            events.select("repo_id", "repo_name", "actor_login")
            .where(F.col("actor_login").isNotNull())
            .distinct()
            .groupBy("repo_id", "repo_name")
            .agg(F.count("*").alias("unique_actors"))
        )
        return additive.join(distinct_actors, on=["repo_id", "repo_name"], how="left").select(
            "repo_id", "repo_name", "event_count", "unique_actors", "avg_payload_size"
        )


@F.udf(returnType=BooleanType())
def _failing_udf(_repo_id: int) -> bool:
    """Runs on a Spark Python worker; the task fails and Spark aborts the job."""
    raise_injected_failure("spark")
    return True
