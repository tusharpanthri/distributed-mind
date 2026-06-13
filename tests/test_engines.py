"""Smoke tests: each engine initialises, runs on synthetic data, and returns a valid BenchmarkResult."""

from __future__ import annotations

import io
import os
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from engines.base import BenchmarkResult

# ---------------------------------------------------------------------------
# Minimal synthetic fixtures
# ---------------------------------------------------------------------------

EVENTS_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string()),
        pa.field("type", pa.string()),
        pa.field("actor_login", pa.string()),
        pa.field("repo_id", pa.int64()),
        pa.field("repo_name", pa.string()),
        pa.field("created_at", pa.timestamp("us", tz="UTC")),
        pa.field("payload_size", pa.int64()),
        pa.field("payload_action", pa.string()),
    ]
)

LOOKUP_SCHEMA = pa.schema(
    [
        pa.field("repo_id", pa.int64()),
        pa.field("language", pa.string()),
        pa.field("repo_owner_type", pa.string()),
    ]
)

_EVENTS_ROWS = [
    {"id": "1", "type": "PushEvent", "actor_login": "alice", "repo_id": 1,
     "repo_name": "org/repo-a", "created_at": None, "payload_size": 3, "payload_action": None},
    {"id": "2", "type": "PushEvent", "actor_login": "bob", "repo_id": 1,
     "repo_name": "org/repo-a", "created_at": None, "payload_size": 1, "payload_action": None},
    {"id": "3", "type": "PushEvent", "actor_login": "carol", "repo_id": 2,
     "repo_name": "org/repo-b", "created_at": None, "payload_size": 5, "payload_action": None},
    {"id": "4", "type": "WatchEvent", "actor_login": "dave", "repo_id": 2,
     "repo_name": "org/repo-b", "created_at": None, "payload_size": None, "payload_action": "started"},
]

_LOOKUP_ROWS = [
    {"repo_id": 1, "language": "Python", "repo_owner_type": "Organization"},
]


def _make_parquet_bytes(rows: list[dict], schema: pa.Schema) -> bytes:
    table = pa.Table.from_pylist(rows, schema=schema)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


EVENTS_BYTES = _make_parquet_bytes(_EVENTS_ROWS, EVENTS_SCHEMA)
LOOKUP_BYTES = _make_parquet_bytes(_LOOKUP_ROWS, LOOKUP_SCHEMA)


# ---------------------------------------------------------------------------
# Shared config stub
# ---------------------------------------------------------------------------

def _test_config() -> dict[str, Any]:
    return {
        "minio": {
            "endpoint": "http://localhost:9000",
            "access_key": "minioadmin",
            "secret_key": "minioadmin",
            "bucket_raw": "raw-data",
            "bucket_output": "processed-data",
        },
        "benchmark": {
            "input_path": "s3a://raw-data/events",
            "output_path": "s3a://processed-data/output",
            "lookup_path": "s3a://raw-data/lookup/repo_metadata.parquet",
        },
        "spark": {"master": "local[1]", "app_name": "test"},
        "dask": {"n_workers": 1, "threads_per_worker": 1, "memory_limit": "512MB"},
        "ray": {"num_cpus": 1, "object_store_memory": 268435456},
    }


# ---------------------------------------------------------------------------
# BenchmarkResult unit tests (no engine needed)
# ---------------------------------------------------------------------------

def test_benchmark_result_rows_per_second() -> None:
    r = BenchmarkResult(
        engine_name="test",
        duration_seconds=10.0,
        rows_processed=1000,
        rows_output=50,
        peak_memory_mb=100.0,
        success=True,
    )
    assert r.rows_per_second == pytest.approx(100.0)


def test_benchmark_result_zero_duration() -> None:
    r = BenchmarkResult(
        engine_name="test",
        duration_seconds=0.0,
        rows_processed=1000,
        rows_output=50,
        peak_memory_mb=0.0,
        success=True,
    )
    assert r.rows_per_second == 0.0


# ---------------------------------------------------------------------------
# Engine import smoke tests
# ---------------------------------------------------------------------------

def test_spark_engine_importable() -> None:
    from engines.spark_engine import SparkEngine
    assert SparkEngine is not None


def test_dask_engine_importable() -> None:
    from engines.dask_engine import DaskEngine
    assert DaskEngine is not None


def test_ray_engine_importable() -> None:
    from engines.ray_engine import RayEngine
    assert RayEngine is not None
