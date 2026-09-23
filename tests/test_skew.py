"""Skew amplification and mitigation.

Unit tests cover the amplifier, the slowdown-ratio calculation, and the
partial-aggregation helpers used by the mitigated code paths. Integration
tests check the skewed dataset in MinIO and that every engine's mitigated
output is identical to its unmitigated output — mitigation must change how
the work is distributed, never the answer.
"""

from __future__ import annotations

import os
from typing import Any

import pandas as pd
import pyarrow as pa
import pytest

from data.amplify_skew import amplify, matching_rows, skew_stats, top_keys
from data.download_gharchive_data import FLAT_SCHEMA
from engines.base import BenchmarkResult
from workloads.spec import load_workload

SPEC = load_workload("config/workloads/gharchive-repo-activity.yaml")

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _events_table() -> pa.Table:
    rows = []
    # repo 1: 6 pushes (hot), repo 2: 3 pushes, repos 3-6: 1 push each, repo 9: watch-only
    for i, (repo, actor) in enumerate(
        [(1, "a"), (1, "b"), (1, "a"), (1, "c"), (1, "a"), (1, "bot"),
         (2, "d"), (2, "d"), (2, "e"), (3, "f"), (4, "g"), (5, "h"), (6, "i")]
    ):
        rows.append({"id": str(i), "type": "PushEvent", "actor_login": actor, "repo_id": repo,
                     "repo_name": f"org/r{repo}", "created_at": None, "payload_size": i % 4,
                     "payload_action": None})
    for i in range(20):
        rows.append({"id": f"w{i}", "type": "WatchEvent", "actor_login": f"s{i}", "repo_id": 9,
                     "repo_name": "org/r9", "created_at": None, "payload_size": None,
                     "payload_action": "started"})
    return pa.Table.from_pylist(rows, schema=FLAT_SCHEMA)


def _result(engine: str, dataset: str, mitigation: bool, duration: float) -> BenchmarkResult:
    return BenchmarkResult(engine_name=engine, duration_seconds=duration, rows_processed=1,
                           rows_output=1, peak_memory_mb=0.0, success=True,
                           dataset_type=dataset, mitigation_applied=mitigation)


# ---------------------------------------------------------------------------
# Amplifier
# ---------------------------------------------------------------------------


def test_hot_repos_ranked_by_push_events_only() -> None:
    # repo 9 has the most rows overall, but only WatchEvents
    assert top_keys(matching_rows(_events_table(), SPEC), SPEC, 2) == [1, 2]


def test_amplify_replicates_only_hot_rows() -> None:
    table = _events_table()
    skewed = amplify(table, [1], SPEC, skew_factor=10)
    counts = skewed.to_pandas()["repo_id"].value_counts()

    assert counts[1] == 6 * 10
    assert counts[2] == 3
    assert counts[9] == 20
    assert skewed.num_rows == table.num_rows + 6 * 9


def test_amplified_rows_get_unique_ids_and_keep_actors() -> None:
    skewed = amplify(_events_table(), [1], SPEC, skew_factor=4).to_pandas()
    assert skewed["id"].is_unique
    hot = skewed[skewed["repo_id"] == 1]
    assert set(hot["actor_login"]) == {"a", "b", "c", "bot"}  # distinct actors unchanged


def test_skew_factor_one_is_identity() -> None:
    table = _events_table()
    assert amplify(table, [1], SPEC, skew_factor=1).num_rows == table.num_rows


def test_amplify_rejects_invalid_factor() -> None:
    with pytest.raises(ValueError):
        amplify(_events_table(), [1], SPEC, skew_factor=0)


def test_skew_stats_increase_after_amplification() -> None:
    pushes = matching_rows(_events_table(), SPEC)
    before = skew_stats(pushes, SPEC, top_n=1)
    after = skew_stats(amplify(pushes, [1], SPEC, skew_factor=10), SPEC, top_n=1)

    assert before.top_repo_share == pytest.approx(6 / 13)
    assert after.top_repo_share == pytest.approx(60 / 67)
    assert after.max_to_median_ratio > before.max_to_median_ratio
    assert after.distinct_repos == before.distinct_repos


# ---------------------------------------------------------------------------
# Slowdown ratio
# ---------------------------------------------------------------------------


def test_skew_slowdown_ratio_per_engine_and_mitigation() -> None:
    from benchmark.runner import skew_slowdown_ratios

    ratios = skew_slowdown_ratios([
        _result("dask", "balanced", False, 2.0),
        _result("dask", "skewed", False, 5.0),
        _result("dask", "balanced", True, 2.5),
        _result("dask", "skewed", True, 3.0),
        _result("ray", "skewed", False, 9.0),  # no balanced baseline → no ratio
    ])
    assert ratios == {("dask", False): pytest.approx(2.5), ("dask", True): pytest.approx(1.2)}


# ---------------------------------------------------------------------------
# Mitigation helpers are exact
# ---------------------------------------------------------------------------


def _reference_aggregate(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(["repo_id", "repo_name"])
        .agg(event_count=("id", "size"), unique_actors=("actor_login", "nunique"),
             avg_payload_size=("payload_size", "mean"))
        .reset_index()
        .sort_values("repo_id", ignore_index=True)
    )


def test_ray_mitigated_pipeline_is_exact() -> None:
    from engines.ray_engine import (
        _aggregate_bucket,
        _aggregate_partials,
        _assign_bucket,
        _combine_buckets,
        _partial_aggregate,
    )

    df = matching_rows(amplify(_events_table(), [1], SPEC, 5), SPEC).to_pandas()
    blocks = (df.iloc[:10], df.iloc[10:25], df.iloc[25:])

    # Unmitigated: bucket by group key, aggregate each bucket.
    keyed = pd.concat(_assign_bucket(b, num_buckets=4, key_columns=SPEC.group_by) for b in blocks)
    direct = pd.concat(_aggregate_bucket(g, spec=SPEC) for _, g in keyed.groupby("bucket"))

    # Mitigated: map-side partials, hot key spread across buckets by the
    # distinct column, then per-bucket aggregation and a final combine.
    partials = pd.concat(_partial_aggregate(b, spec=SPEC) for b in blocks)
    spread = _assign_bucket(partials, num_buckets=4, key_columns=SPEC.group_by,
                            hot_keys={1}, skew_key=SPEC.skew_key, spread_column="actor_login")
    assert spread.loc[spread["repo_id"] == 1, "bucket"].nunique() > 1
    per_bucket = pd.concat(_aggregate_partials(g, spec=SPEC) for _, g in spread.groupby("bucket"))
    combined = _combine_buckets(per_bucket, SPEC)
    cols = ["repo_id", "repo_name", "event_count", "unique_actors", "avg_payload_size"]

    expected = _reference_aggregate(df)
    for got in (direct, combined):
        pd.testing.assert_frame_equal(got[cols].sort_values("repo_id", ignore_index=True), expected,
                                      check_dtype=False)


def test_dask_salt_only_touches_hot_repos() -> None:
    from engines.dask_engine import _add_salt

    df = matching_rows(amplify(_events_table(), [1], SPEC, 5), SPEC).to_pandas()
    salted = _add_salt(df, {1}, 4, SPEC.skew_key, SPEC.row_id)

    assert set(salted.loc[salted["repo_id"] != 1, "salt"]) == {0}
    assert salted.loc[salted["repo_id"] == 1, "salt"].nunique() > 1
    assert salted["salt"].between(0, 3).all()
    # deterministic: same id → same salt
    pd.testing.assert_series_equal(
        salted["salt"], _add_salt(df, {1}, 4, SPEC.skew_key, SPEC.row_id)["salt"]
    )


# ---------------------------------------------------------------------------
# Integration tests (real engines against MinIO)
# ---------------------------------------------------------------------------


def _cfg() -> dict[str, Any]:
    from benchmark.runner import _load_config

    return _load_config(os.environ.get("BENCHMARK_CONFIG", "config/benchmark_config.yaml"))


def _read(path: str, cfg: dict[str, Any], **kwargs: Any) -> pd.DataFrame:
    import pyarrow.fs as pafs
    import pyarrow.parquet as pq

    minio = cfg["minio"]
    fs = pafs.S3FileSystem(access_key=minio["access_key"], secret_key=minio["secret_key"],
                           endpoint_override=minio["endpoint"].split("://")[-1], scheme="http")
    return pq.read_table(path.split("://")[-1], filesystem=fs, **kwargs).to_pandas()


@pytest.mark.integration
@pytest.mark.skipif(not os.environ.get("MINIO_ENDPOINT"), reason="MINIO_ENDPOINT not set")
def test_skewed_dataset_has_amplified_distribution() -> None:
    cfg = _cfg()
    columns = ["id", "type", "repo_id"]
    balanced = _read(cfg["benchmark"]["input_path"], cfg, columns=columns)
    skewed = _read(cfg["benchmark"]["skewed_input_path"], cfg, columns=columns)
    top_n = int(cfg["skew"]["amplify_top_n"])
    factor = int(cfg["skew"]["skew_factor"])

    push_b = balanced[balanced["type"] == "PushEvent"]["repo_id"].value_counts()
    push_s = skewed[skewed["type"] == "PushEvent"]["repo_id"].value_counts()
    # same tie-break as data.amplify_skew.top_repo_ids: count desc, repo_id asc
    ranked = push_b.rename("n").reset_index().sort_values(["n", "repo_id"], ascending=[False, True])
    hot = ranked["repo_id"].head(top_n).tolist()

    assert skewed["id"].is_unique
    assert (push_s[hot] == push_b[hot] * factor).all()
    assert push_s.max() / push_s.sum() > push_b.max() / push_b.sum()
    assert set(push_s.index) == set(push_b.index)


@pytest.mark.integration
@pytest.mark.skipif(not os.environ.get("MINIO_ENDPOINT"), reason="MINIO_ENDPOINT not set")
@pytest.mark.parametrize("engine_name", ["spark", "dask", "ray"])
def test_mitigated_output_matches_unmitigated(engine_name: str) -> None:
    from benchmark.runner import _build_engine

    cfg = _cfg()
    base = f"{cfg['benchmark']['output_path']}/skewtest-{engine_name}"
    engine = _build_engine(engine_name)
    engine.setup(cfg)
    try:
        frames = {}
        for mitigate in (False, True):
            out = f"{base}-{'on' if mitigate else 'off'}"
            result = engine.run(cfg["benchmark"]["skewed_input_path"], out,
                                dataset_type="skewed", mitigate_skew=mitigate)
            assert result.success, result.error_message
            assert result.mitigation_applied is mitigate
            frames[mitigate] = _read(out, cfg)
    finally:
        engine.teardown()

    cols = ["repo_id", "repo_name", "event_count", "unique_actors", "avg_payload_size", "language"]
    off, on = (
        frames[m][cols].astype({"language": str}).sort_values(["repo_id", "repo_name"], ignore_index=True)
        for m in (False, True)
    )
    pd.testing.assert_frame_equal(on, off, check_dtype=False, rtol=1e-9)
