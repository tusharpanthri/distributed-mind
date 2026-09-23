"""PySpark engine — local mode with S3A connector wired to MinIO."""

from __future__ import annotations

import logging
import socket
import time
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

from engines.base import BenchmarkEngine, raise_injected_failure, workload_of
from workloads.spec import PartialField, WorkloadSpec

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
        master = spark_cfg.get("master") or "local[*]"

        builder = (
            SparkSession.builder.appName(spark_cfg.get("app_name", "DistributedMind"))
            .master(master)
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
        )

        if not master.startswith("local"):
            # Standalone cluster: executors dial back to this driver, so the
            # driver advertises its container hostname (Docker DNS resolves it)
            # on fixed ports rather than a random one.
            cluster = self._config.get("cluster", {})
            cores = int(cluster.get("worker_cores", 2)) * max(1, int(cluster.get("workers", 1)))
            builder = (
                builder.config("spark.driver.host", socket.gethostname())
                .config("spark.driver.port", "7078")
                .config("spark.blockManager.port", "7079")
                .config("spark.cores.max", str(cores))
                .config("spark.sql.shuffle.partitions", str(max(8, 2 * cores)))
                .config("spark.executor.memory", spark_cfg.get("executor_memory", "1500m"))
            )

        self._spark = builder.getOrCreate()
        self._spark.sparkContext.setLogLevel("WARN")
        if not master.startswith("local"):
            self._wait_for_executors(timeout=120)
        logger.info("Spark session created", extra={"master": master})

    def _wait_for_executors(self, timeout: float, poll: float = 2.0) -> None:
        """Block until at least one executor registers, else fail fast.

        A standalone job submitted to a master with no live workers would
        otherwise sit in the queue indefinitely.
        """
        assert self._spark is not None
        sc = self._spark.sparkContext._jsc.sc()
        deadline = time.time() + timeout
        while time.time() < deadline:
            # The map includes the driver, so >1 means a real executor registered.
            if sc.getExecutorMemoryStatus().size() > 1:
                return
            time.sleep(poll)
        raise RuntimeError(f"no Spark executors registered within {timeout:.0f}s")

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
        spec = workload_of(self._config)

        # 1. Read only the columns this workload needs
        events = spark.read.parquet(input_path).select(*spec.input_columns)

        # 2. Filter per the spec
        for condition in _filter_conditions(spec):
            events = events.filter(condition)
        if inject_failure:
            events = events.filter(_failing_udf(F.col(spec.group_by[0])))
        # Filtered events are materialized once, then counted and aggregated
        # (Dask persists and Ray materializes at the same point).
        events = events.cache()
        rows_processed = events.count()

        # 3. Aggregate per the spec
        if mitigate_skew:
            aggregated = self._aggregate_salted(events, spec)
        else:
            aggregated = events.groupBy(*spec.group_by).agg(*_direct_aggs(spec))

        # 4. Join the lookup table (left join → spec's `missing` for absent keys)
        if spec.join:
            lookup = spark.read.parquet(lookup_path).select(spec.join.key, *spec.join.columns)
            aggregated = aggregated.join(lookup, on=spec.join.key, how="left").fillna(
                {column: spec.join.missing for column in spec.join.columns}
            )
        enriched = aggregated.select(*spec.output_columns)

        # 5. Write, partitioned as the spec asks
        writer = enriched.write.mode("overwrite")
        if spec.partition_by:
            writer = writer.partitionBy(spec.partition_by)
        writer.parquet(output_path)

        rows_output = enriched.count()
        events.unpersist()
        return rows_processed, rows_output

    def _aggregate_salted(self, events: DataFrame, spec: WorkloadSpec) -> DataFrame:
        """Salting: split each hot key into N sub-keys, aggregate, then recombine.

        Additive aggregates (count, sum, min, max, and mean carried as sum +
        non-null count) are computed per (key, salt) and combined. A distinct
        count can't be summed across random salts, so it is computed by first
        de-duplicating (keys, column): that shuffle is keyed on the column too,
        so a hot key's rows spread out, and map-side dedupe collapses the
        repeat offenders (bot accounts pushing thousands of times).
        """
        skew_cfg = self._config.get("skew", {})
        top_n = int(skew_cfg.get("hot_key_top_n", 10))
        buckets = int(skew_cfg.get("salt_buckets", 8))

        hot_keys = [
            row[spec.skew_key]
            for row in events.groupBy(spec.skew_key).count()
            .orderBy(F.desc("count")).limit(top_n).collect()
        ]
        salt = (
            F.when(F.col(spec.skew_key).isin(hot_keys), F.pmod(F.hash(spec.row_id), F.lit(buckets)))
            .otherwise(F.lit(0))
        )

        partial_exprs = []
        for aggregation in spec.additive_aggregations:
            for partial_field in aggregation.partial_fields():
                partial_exprs.append(_partial_expr(partial_field).alias(partial_field.name))
        partials = (
            events.withColumn("salt", salt)
            .groupBy(*spec.group_by, "salt")
            .agg(*partial_exprs)
        )

        combine_exprs = []
        for aggregation in spec.additive_aggregations:
            fields = aggregation.partial_fields()
            if aggregation.op == "mean":
                total, count = fields
                combine_exprs.append((F.sum(total.name) / F.sum(count.name)).alias(aggregation.name))
            else:
                combiner = getattr(F, aggregation.combine_op)
                combine_exprs.append(combiner(fields[0].name).alias(aggregation.name))
        result = partials.groupBy(*spec.group_by).agg(*combine_exprs)

        for aggregation in spec.distinct_aggregations:
            distinct = (
                events.select(*spec.group_by, aggregation.value_column)
                .where(F.col(aggregation.value_column).isNotNull())
                .distinct()
                .groupBy(*spec.group_by)
                .agg(F.count("*").alias(aggregation.name))
            )
            result = result.join(distinct, on=spec.group_by, how="left").withColumn(
                aggregation.name, F.coalesce(F.col(aggregation.name), F.lit(0))
            )

        return result.select(*spec.group_by, *(a.name for a in spec.aggregations))


def _filter_conditions(spec: WorkloadSpec) -> list[Column]:
    conditions = []
    if spec.filter:
        conditions.append(F.col(spec.filter.column) == F.lit(spec.filter.equals))
    conditions.extend(F.col(column).isNotNull() for column in spec.require_not_null)
    return conditions


def _direct_aggs(spec: WorkloadSpec) -> list[Column]:
    exprs = []
    for aggregation in spec.aggregations:
        if aggregation.op == "count":
            expr = F.count("*")
        elif aggregation.op == "distinct_count":
            expr = F.countDistinct(aggregation.value_column)
        elif aggregation.op == "mean":
            expr = F.avg(aggregation.value_column)
        else:  # sum, min, max
            expr = getattr(F, aggregation.op)(aggregation.value_column)
        exprs.append(expr.alias(aggregation.name))
    return exprs


def _partial_expr(partial: PartialField) -> Column:
    if partial.op == "count":
        # Counting the column (not *) is what makes it a non-null count, which
        # is the denominator a mean needs.
        return F.count(partial.column) if partial.column else F.count("*")
    return getattr(F, partial.op)(partial.value_column)


@F.udf(returnType=BooleanType())
def _failing_udf(_repo_id: int) -> bool:
    """Runs on a Spark Python worker; the task fails and Spark aborts the job."""
    raise_injected_failure("spark")
    return True
