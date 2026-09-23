"""S3/MinIO access shared by the runner, the data scripts, and the Ray engine."""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pyarrow.dataset as pads
import pyarrow.fs as pafs


def strip_scheme(uri: str) -> str:
    """``s3a://bucket/key`` (or ``s3://``) -> ``bucket/key``."""
    return uri.replace("s3a://", "").replace("s3://", "")


def s3_filesystem(minio: dict[str, Any]) -> pafs.S3FileSystem:
    """A pyarrow filesystem for the configured endpoint (MinIO or real S3)."""
    endpoint = minio["endpoint"]
    return pafs.S3FileSystem(
        access_key=minio["access_key"],
        secret_key=minio["secret_key"],
        endpoint_override=strip_scheme(endpoint),
        scheme="https" if endpoint.startswith("https") else "http",
    )


def dataset_schema(path: str, minio: dict[str, Any]) -> pa.Schema:
    """Unified schema of a Parquet dataset, without reading any rows."""
    return pads.dataset(
        strip_scheme(path),
        filesystem=s3_filesystem(minio),
        format="parquet",
        partitioning="hive",
    ).schema
