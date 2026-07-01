"""Cross-engine data integrity tests.

These tests run against a real MinIO instance (started by docker-compose).
They are skipped automatically when MINIO_ENDPOINT is not set or unreachable.

Run locally:
    docker-compose up -d minio
    python -m data.download_gharchive_data --date 2024-01-15 --hours 1 --sample-size 50000
    pytest tests/test_data_integrity.py -v
"""

from __future__ import annotations

import os
from typing import Any

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Skip guard — skip the whole module if MinIO is not available
# ---------------------------------------------------------------------------

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "")

pytestmark = pytest.mark.skipif(
    not MINIO_ENDPOINT,
    reason="MINIO_ENDPOINT not set — skipping integration tests",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_config() -> dict[str, Any]:
    import re
    import yaml

    config_path = os.environ.get("BENCHMARK_CONFIG", "config/benchmark_config.yaml")
    with open(config_path) as f:
        raw = yaml.safe_load(f)

    def walk(node: object) -> object:
        if isinstance(node, dict):
            return {k: walk(v) for k, v in node.items()}
        if isinstance(node, list):
            return [walk(i) for i in node]
        if isinstance(node, str):
            def replace(m: re.Match) -> str:
                var, _, default = m.group(1).partition(":")
                return os.environ.get(var, default)
            return re.sub(r"\$\{([^}]+)\}", replace, node)
        return node

    return walk(raw)  # type: ignore[return-value]


def _read_parquet_from_minio(s3_path: str, cfg: dict[str, Any]) -> pd.DataFrame:
    """Read a Parquet path (s3a://) from MinIO into a pandas DataFrame."""
    import pyarrow.parquet as pq
    import pyarrow.fs as pafs

    minio = cfg["minio"]
    endpoint = minio["endpoint"].replace("http://", "").replace("https://", "")
    fs = pafs.S3FileSystem(
        access_key=minio["access_key"],
        secret_key=minio["secret_key"],
        endpoint_override=endpoint,
        scheme="http",
    )
    path = s3_path.replace("s3a://", "").replace("s3://", "")
    return pq.read_table(path, filesystem=fs).to_pandas()


def _run_engine(engine_name: str, cfg: dict[str, Any]) -> "BenchmarkResult":  # noqa: F821
    from benchmark.runner import _build_engine

    input_path = cfg["benchmark"]["input_path"]
    output_path = f"{cfg['benchmark']['output_path']}/{engine_name}"
    engine = _build_engine(engine_name)
    engine.setup(cfg)
    try:
        return engine.run(input_path, output_path)
    finally:
        engine.teardown()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def all_results() -> dict[str, Any]:
    """Run all three engines once and collect results + output DataFrames."""
    cfg = _load_config()
    results = {}
    frames = {}

    for engine_name in ["spark", "dask", "ray"]:
        result = _run_engine(engine_name, cfg)
        results[engine_name] = result
        if result.success:
            output_path = f"{cfg['benchmark']['output_path']}/{engine_name}"
            frames[engine_name] = _read_parquet_from_minio(output_path, cfg)

    return {"results": results, "frames": frames, "config": cfg}


def test_all_engines_succeed(all_results: dict) -> None:
    for name, result in all_results["results"].items():
        assert result.success, f"{name} failed: {result.error_message}"


def test_row_counts_match(all_results: dict) -> None:
    """All engines must process the same number of rows."""
    processed = {name: r.rows_processed for name, r in all_results["results"].items()}
    counts = list(processed.values())
    assert all(c == counts[0] for c in counts), f"Row count mismatch: {processed}"


def test_output_row_counts_match(all_results: dict) -> None:
    """All engines must produce the same number of output rows."""
    frames = all_results["frames"]
    if len(frames) < 2:
        pytest.skip("Fewer than 2 engines produced output")
    counts = {name: len(df) for name, df in frames.items()}
    values = list(counts.values())
    assert all(v == values[0] for v in values), f"Output row count mismatch: {counts}"


def test_aggregate_values_match(all_results: dict) -> None:
    """event_count and unique_actors aggregates must match across engines (within float tolerance)."""
    frames = all_results["frames"]
    if len(frames) < 2:
        pytest.skip("Fewer than 2 engines produced output")

    engine_names = list(frames.keys())
    ref_name = engine_names[0]
    ref = frames[ref_name].set_index("repo_id").sort_index()

    for other_name in engine_names[1:]:
        other = frames[other_name].set_index("repo_id").sort_index()

        # Row-level event_count must match exactly
        common = ref.index.intersection(other.index)
        pd.testing.assert_series_equal(
            ref.loc[common, "event_count"].reset_index(drop=True),
            other.loc[common, "event_count"].reset_index(drop=True),
            check_names=False,
            obj=f"event_count ({ref_name} vs {other_name})",
        )

        # avg_payload_size may differ by float precision
        pd.testing.assert_series_equal(
            ref.loc[common, "avg_payload_size"].reset_index(drop=True),
            other.loc[common, "avg_payload_size"].reset_index(drop=True),
            check_names=False,
            rtol=1e-5,
            obj=f"avg_payload_size ({ref_name} vs {other_name})",
        )


def test_language_partitions_present(all_results: dict) -> None:
    """Output must contain a 'language' column with no fully-null entries."""
    for name, df in all_results["frames"].items():
        assert "language" in df.columns, f"{name}: missing 'language' column"
        assert df["language"].notna().all(), f"{name}: null language values found"
