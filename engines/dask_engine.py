"""Dask engine — LocalCluster with S3 storage via s3fs wired to MinIO."""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urlparse

import dask
import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client, LocalCluster
from minio import Minio
from minio.deleteobjects import DeleteObject

from engines.base import BenchmarkEngine, raise_injected_failure, workload_of
from workloads.spec import Aggregation, WorkloadSpec

logger = logging.getLogger("distributedmind.dask")


class DaskEngine(BenchmarkEngine):
    """Runs the benchmark transformation using Dask with a LocalCluster."""

    name = "dask"

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
        address = dask_cfg.get("scheduler_address") or ""

        if address:
            # Shared scheduler owned by docker-compose: connect, never create.
            self._client = Client(address, timeout=60)
            self._client.wait_for_workers(1, timeout=120)
            logger.info("Dask cluster connected",
                        extra={"address": address, "workers": len(self._client.scheduler_info()["workers"])})
        else:
            self._cluster = LocalCluster(
                n_workers=dask_cfg.get("n_workers", 2),
                threads_per_worker=dask_cfg.get("threads_per_worker", 2),
                memory_limit=dask_cfg.get("memory_limit", "2GB"),
            )
            self._client = Client(self._cluster)
            logger.info("Dask LocalCluster created", extra={"dashboard": self._client.dashboard_link})

    def teardown(self) -> None:
        # Only the LocalCluster is ours to shut down; a shared scheduler
        # outlives the run, so we just disconnect the client.
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

    def _available_cores(self) -> int:
        """Worker threads the cluster actually has (local mode: the configured count)."""
        dask_cfg = self._config.get("dask", {})
        if dask_cfg.get("scheduler_address") and self._client is not None:
            workers = self._client.scheduler_info()["workers"]
            if workers:
                return sum(int(w["nthreads"]) for w in workers.values())
        return int(dask_cfg.get("n_workers", 2)) * int(dask_cfg.get("threads_per_worker", 2))

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

    def _transform(
        self,
        input_path: str,
        output_path: str,
        lookup_path: str,
        mitigate_skew: bool,
        inject_failure: bool,
    ) -> tuple[int, int]:
        spec = workload_of(self._config)
        minio = self._config["minio"]
        storage_options = {
            "key": minio["access_key"],
            "secret": minio["secret_key"],
            "endpoint_url": minio["endpoint"],
        }

        # Convert s3a:// -> s3:// (s3fs uses s3://)
        s3_input = input_path.replace("s3a://", "s3://")
        s3_output = output_path.replace("s3a://", "s3://")
        s3_lookup = lookup_path.replace("s3a://", "s3://")

        # 1. Read only the columns this workload needs
        events = dd.read_parquet(s3_input, storage_options=storage_options,
                                 columns=spec.input_columns)

        # 2. Filter per the spec
        if spec.filter:
            events = events[events[spec.filter.column] == spec.filter.equals]
        for column in spec.require_not_null:
            events = events[events[column].notnull()]
        if inject_failure:
            events = events.map_partitions(_failing_partition, meta=events._meta)
        # Filtered events are materialized once, then counted and aggregated
        # (Spark caches and Ray materializes at the same point).
        events = events.persist()
        rows_processed = int(len(events))

        # 3. Aggregate per the spec.
        # Both branches return pandas: the groupbys/shuffles run distributed,
        # but their result is one row per group, and merging two dask groupby
        # results directly trips a dask-expr optimizer KeyError
        # ("['repo_id' 'repo_name'] not in index") once the input spans several
        # partitions. Ray's engine assembles its output the same way.
        if mitigate_skew:
            aggregated_pd = self._aggregate_repartitioned(events, spec)
        else:
            aggregated_pd = self._aggregate_direct(events, spec)

        # 4. Join the lookup table (left join -> spec's `missing` for absent keys)
        if spec.join:
            lookup_pd: pd.DataFrame = dd.read_parquet(
                s3_lookup, storage_options=storage_options,
                columns=[spec.join.key, *spec.join.columns],
            ).compute()
            aggregated_pd = aggregated_pd.merge(lookup_pd, on=spec.join.key, how="left")
            for column in spec.join.columns:
                aggregated_pd[column] = aggregated_pd[column].fillna(spec.join.missing)
        aggregated_pd = _normalize_count_dtypes(aggregated_pd, spec)
        enriched = aggregated_pd[spec.output_columns]

        # 5. Write, partitioned as the spec asks.
        # Prefix is cleared explicitly first (see _clear_output_prefix); the
        # write itself uses overwrite=False so dask never triggers s3fs's
        # bulk-delete path.
        self._clear_output_prefix(s3_output)
        dd.from_pandas(enriched, npartitions=max(1, self._available_cores())).to_parquet(
            s3_output,
            partition_on=[spec.partition_by] if spec.partition_by else None,
            storage_options=storage_options,
            write_index=False,
            overwrite=False,
        )

        rows_output = int(len(enriched))
        return rows_processed, rows_output

    def _aggregate_direct(self, events: dd.DataFrame, spec: WorkloadSpec) -> pd.DataFrame:
        """One groupby per spec.

        Distinct counts are separate groupby calls: "nunique" isn't accepted
        inside groupby().agg() on this dask-expr version.
        """
        named = {a.name: _pandas_agg(a, spec) for a in spec.additive_aggregations}
        frames = [events.groupby(spec.group_by).agg(**named).reset_index()]
        for aggregation in spec.distinct_aggregations:
            frames.append(
                events.groupby(spec.group_by)[aggregation.column]
                .nunique()
                .rename(aggregation.name)
                .reset_index()
            )
        # One graph, so the filtered events are traversed once for all of them.
        computed = dask.compute(*frames)
        result = computed[0]
        for frame in computed[1:]:
            result = result.merge(frame, on=spec.group_by, how="left")
        return result

    def _aggregate_repartitioned(self, events: dd.DataFrame, spec: WorkloadSpec) -> pd.DataFrame:
        """Repartition informed by the key distribution before the groupby.

        Input is first rebalanced into evenly sized partitions (one big
        Parquet file otherwise lands in a single partition). Rows of the top-N
        hot keys get a salt so they hash to different output partitions
        (``split_out``) instead of piling onto one, then partials are combined.
        Distinct counts come from de-duplicated (keys, column) pairs, which
        shuffle on the column too and so don't concentrate on a hot key.
        """
        skew_cfg = self._config.get("skew", {})
        top_n = int(skew_cfg.get("hot_key_top_n", 10))
        buckets = int(skew_cfg.get("salt_buckets", 8))
        n_parts = max(2, 2 * self._available_cores())

        hot_keys = set(events[spec.skew_key].value_counts().nlargest(top_n).index.compute().tolist())
        events = events.repartition(npartitions=n_parts)
        salted = events.map_partitions(_add_salt, hot_keys, buckets, spec.skew_key, spec.row_id)

        partial_named = {}
        for aggregation in spec.additive_aggregations:
            for partial_field in aggregation.partial_fields():
                partial_named[partial_field.name] = (
                    (spec.row_id, "size") if partial_field.column is None
                    else (partial_field.value_column, partial_field.op)
                )
        partial = (
            salted.groupby([*spec.group_by, "salt"])
            .agg(**partial_named, split_out=n_parts)
            .reset_index()
        )
        # The combine stage sees at most ``buckets`` partial rows per group, so it
        # needs no split_out (and a second split_out here trips a dask-expr
        # optimizer KeyError when the result is merged below).
        combine_named = {
            field.name: (field.name, "sum" if field.op == "count" else field.op)
            for aggregation in spec.additive_aggregations
            for field in aggregation.partial_fields()
        }
        additive = partial.groupby(spec.group_by).agg(**combine_named).reset_index()

        distinct_frames = [
            events[[*spec.group_by, aggregation.column]]
            .dropna(subset=[aggregation.column])
            .drop_duplicates(split_out=n_parts)
            .groupby(spec.group_by)
            .size(split_out=n_parts)
            .rename(aggregation.name)
            .reset_index()
            for aggregation in spec.distinct_aggregations
        ]
        computed = dask.compute(additive, *distinct_frames)
        result = computed[0]
        for frame in computed[1:]:
            result = result.merge(frame, on=spec.group_by, how="left")

        for aggregation in spec.additive_aggregations:
            fields = aggregation.partial_fields()
            if aggregation.op == "mean":
                total, count = fields
                result[aggregation.name] = result[total.name] / result[count.name].where(
                    result[count.name] > 0
                )
            else:
                result[aggregation.name] = result[fields[0].name]
        return result[[*spec.group_by, *(a.name for a in spec.aggregations)]]


def _normalize_count_dtypes(frame: pd.DataFrame, spec: WorkloadSpec) -> pd.DataFrame:
    """Counts as plain int64.

    A "size" aggregation over pyarrow-backed columns yields pandas' nullable
    Int64, which survives into the Parquet output and makes dtypes differ from
    Spark and Ray for identical values. A group with no non-null values counts
    as 0 rather than null.
    """
    for aggregation in spec.aggregations:
        if aggregation.op in ("count", "distinct_count"):
            frame[aggregation.name] = frame[aggregation.name].fillna(0).astype("int64")
    return frame


def _pandas_agg(aggregation: "Aggregation", spec: WorkloadSpec) -> tuple[str, str]:
    """Named-aggregation tuple for dask/pandas ``agg``.

    A plain row count has no column of its own, so it counts the row-id column
    with "size"; ``count`` over a value column means non-null values, which is
    the denominator a mean needs.
    """
    if aggregation.op == "count":
        return (spec.row_id, "size")
    return (aggregation.value_column, aggregation.op)


def _add_salt(part: pd.DataFrame, hot_keys: set[Any], buckets: int,
              skew_key: str, row_id: str) -> pd.DataFrame:
    salt = pd.util.hash_pandas_object(part[row_id], index=False).to_numpy() % buckets
    part = part.assign(salt=salt.astype("int64"))
    part.loc[~part[skew_key].isin(hot_keys), "salt"] = 0
    return part


def _failing_partition(part: pd.DataFrame) -> pd.DataFrame:
    """Runs on a Dask worker; the task errors and dask surfaces it on compute."""
    if len(part):
        raise_injected_failure("dask")
    return part
