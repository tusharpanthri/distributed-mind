"""Workload spec parsing, validation, and aggregation decomposition."""

from __future__ import annotations

from typing import Any

import pandas as pd
import pyarrow as pa
import pytest

from workloads.spec import (
    WorkloadSchemaError,
    WorkloadSpecError,
    load_workload,
    parse_workload,
)

SHIPPED = ["config/workloads/gharchive-repo-activity.yaml", "config/workloads/actor-activity.yaml"]


def _raw(**overrides: Any) -> dict[str, Any]:
    spec: dict[str, Any] = {
        "name": "test",
        "row_id": "id",
        "skew_key": "key",
        "columns": {"id": "string", "key": "int64", "value": "int64", "who": "string"},
        "group_by": ["key"],
        "aggregations": [{"name": "n", "op": "count"}],
    }
    spec.update(overrides)
    return spec


# ---------------------------------------------------------------------------
# Shipped specs
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("path", SHIPPED)
def test_shipped_specs_load(path: str) -> None:
    spec = load_workload(path)
    assert spec.aggregations and spec.group_by
    assert spec.row_id in spec.columns and spec.skew_key in spec.columns


def test_default_spec_describes_the_benchmarked_workload() -> None:
    spec = load_workload(SHIPPED[0])
    assert spec.group_by == ["repo_id", "repo_name"]
    assert [a.name for a in spec.aggregations] == ["event_count", "unique_actors", "avg_payload_size"]
    assert spec.join is not None and spec.join.key == "repo_id"
    assert spec.partition_by == "language"


def test_second_spec_has_no_join_and_different_keys() -> None:
    spec = load_workload(SHIPPED[1])
    assert spec.group_by == ["actor_login"]
    assert spec.join is None and spec.partition_by is None
    assert {a.op for a in spec.aggregations} == {"count", "distinct_count", "sum", "max"}


# ---------------------------------------------------------------------------
# Decomposition (what the mitigated paths rely on)
# ---------------------------------------------------------------------------

def test_count_decomposes_to_a_summed_count() -> None:
    agg = parse_workload(_raw()).aggregations[0]
    fields = agg.partial_fields()
    assert [(f.op, f.column) for f in fields] == [("count", None)]
    assert agg.combine_op == "sum"


def test_mean_decomposes_to_sum_and_non_null_count() -> None:
    spec = parse_workload(_raw(aggregations=[{"name": "avg", "op": "mean", "column": "value"}]))
    fields = spec.aggregations[0].partial_fields()
    assert [(f.op, f.column) for f in fields] == [("sum", "value"), ("count", "value")]
    assert spec.aggregations[0].combine_op == "mean"


@pytest.mark.parametrize("op,expected", [("sum", "sum"), ("min", "min"), ("max", "max")])
def test_additive_ops_combine_with_themselves(op: str, expected: str) -> None:
    spec = parse_workload(_raw(aggregations=[{"name": "x", "op": op, "column": "value"}]))
    assert spec.aggregations[0].combine_op == expected


def test_distinct_count_has_no_additive_decomposition() -> None:
    spec = parse_workload(_raw(aggregations=[{"name": "d", "op": "distinct_count", "column": "who"}]))
    agg = spec.aggregations[0]
    assert agg.is_distinct and agg.partial_fields() == []
    assert spec.distinct_aggregations == [agg] and spec.additive_aggregations == []
    with pytest.raises(WorkloadSpecError):
        _ = agg.combine_op


def test_decomposition_matches_a_direct_aggregate() -> None:
    """Partial-then-combine must equal one-shot aggregation."""
    parse_workload(_raw(aggregations=[
        {"name": "n", "op": "count"},
        {"name": "avg", "op": "mean", "column": "value"},
        {"name": "biggest", "op": "max", "column": "value"},
    ]))
    df = pd.DataFrame({"id": list("abcdef"), "key": [1, 1, 1, 2, 2, 3],
                       "value": [1, 2, None, 4, 6, 9], "who": list("pqrstu")})

    direct = df.groupby("key").agg(n=("id", "size"), avg=("value", "mean"),
                                   biggest=("value", "max")).reset_index()
    # Two arbitrary "salts", as the mitigated paths would produce.
    df = df.assign(salt=[0, 1, 0, 1, 0, 1])
    partial = df.groupby(["key", "salt"]).agg(
        n__count=("id", "size"), avg__sum=("value", "sum"),
        avg__count=("value", "count"), biggest__max=("value", "max"),
    ).reset_index()
    combined = partial.groupby("key").agg(
        n=("n__count", "sum"), avg__sum=("avg__sum", "sum"),
        avg__count=("avg__count", "sum"), biggest=("biggest__max", "max"),
    ).reset_index()
    combined["avg"] = combined["avg__sum"] / combined["avg__count"]

    pd.testing.assert_frame_equal(combined[["key", "n", "avg", "biggest"]], direct,
                                  check_dtype=False)


# ---------------------------------------------------------------------------
# Spec validation
# ---------------------------------------------------------------------------

def test_unsupported_op_names_the_supported_ones() -> None:
    with pytest.raises(WorkloadSpecError, match="median"):
        parse_workload(_raw(aggregations=[{"name": "m", "op": "median", "column": "value"}]))


def test_op_needing_a_column_says_so() -> None:
    with pytest.raises(WorkloadSpecError, match="needs a 'column'"):
        parse_workload(_raw(aggregations=[{"name": "s", "op": "sum"}]))


def test_aggregation_on_undeclared_column_is_rejected() -> None:
    with pytest.raises(WorkloadSpecError, match="nope"):
        parse_workload(_raw(aggregations=[{"name": "s", "op": "sum", "column": "nope"}]))


@pytest.mark.parametrize("missing", ["name", "columns", "group_by", "aggregations", "row_id", "skew_key"])
def test_missing_required_field(missing: str) -> None:
    raw = _raw()
    del raw[missing]
    with pytest.raises(WorkloadSpecError, match=missing):
        parse_workload(raw)


def test_undeclared_group_by_column_is_rejected() -> None:
    with pytest.raises(WorkloadSpecError, match="group_by"):
        parse_workload(_raw(group_by=["ghost"]))


def test_duplicate_aggregation_names_are_rejected() -> None:
    with pytest.raises(WorkloadSpecError, match="duplicate"):
        parse_workload(_raw(aggregations=[{"name": "n", "op": "count"},
                                          {"name": "n", "op": "sum", "column": "value"}]))


def test_aggregation_cannot_shadow_a_group_key() -> None:
    with pytest.raises(WorkloadSpecError, match="collide"):
        parse_workload(_raw(aggregations=[{"name": "key", "op": "count"}]))


def test_join_key_must_be_a_group_key() -> None:
    with pytest.raises(WorkloadSpecError, match="join.key"):
        parse_workload(_raw(join={"path": "s3a://x/y", "key": "value", "columns": ["extra"]}))


def test_join_written_with_on_gets_a_pointed_error() -> None:
    """YAML parses a bare `on:` as True, so the spec uses `key` instead."""
    with pytest.raises(WorkloadSpecError, match="'key', not 'on'"):
        parse_workload(_raw(join={"path": "s3a://x/y", "on": "key", "columns": ["extra"]}))


def test_partition_by_must_be_an_output_column() -> None:
    with pytest.raises(WorkloadSpecError, match="partition_by"):
        parse_workload(_raw(partition_by="value"))


def test_unsupported_column_type_is_rejected() -> None:
    with pytest.raises(WorkloadSpecError, match="decimal"):
        parse_workload(_raw(columns={"id": "string", "key": "decimal"}))


# ---------------------------------------------------------------------------
# Dataset validation
# ---------------------------------------------------------------------------

def _schema(**fields: pa.DataType) -> pa.Schema:
    return pa.schema([pa.field(n, t) for n, t in fields.items()])


def test_matching_schema_validates() -> None:
    spec = parse_workload(_raw())
    spec.validate_against(_schema(id=pa.string(), key=pa.int64(), extra=pa.float64()))


def test_narrower_int_still_satisfies_int64() -> None:
    """A real dataset shouldn't be rejected for being int32 where int64 was declared."""
    parse_workload(_raw()).validate_against(_schema(id=pa.large_string(), key=pa.int32()))


def test_missing_column_is_named() -> None:
    spec = parse_workload(_raw())
    with pytest.raises(WorkloadSchemaError, match="missing column 'key'"):
        spec.validate_against(_schema(id=pa.string()))


def test_wrong_type_is_reported_with_both_types() -> None:
    spec = parse_workload(_raw())
    with pytest.raises(WorkloadSchemaError, match="spec declares int64"):
        spec.validate_against(_schema(id=pa.string(), key=pa.string()))


def test_every_problem_is_reported_at_once() -> None:
    spec = parse_workload(_raw(aggregations=[{"name": "s", "op": "sum", "column": "value"}]))
    with pytest.raises(WorkloadSchemaError) as err:
        spec.validate_against(_schema(id=pa.int64()))
    message = str(err.value)
    assert "missing column 'key'" in message
    assert "missing column 'value'" in message
    assert "'id' is int64" in message
    assert "available columns: id" in message


def test_only_referenced_columns_are_required() -> None:
    """'who' is declared but unused, so a dataset without it still validates."""
    spec = parse_workload(_raw())
    assert "who" not in spec.input_columns
    spec.validate_against(_schema(id=pa.string(), key=pa.int64()))


# ---------------------------------------------------------------------------
# Integration: a different spec runs on every engine without touching engine code
# ---------------------------------------------------------------------------

@pytest.mark.integration
@pytest.mark.skipif(not __import__("os").environ.get("MINIO_ENDPOINT"), reason="MINIO_ENDPOINT not set")
@pytest.mark.parametrize("mitigate", [False, True])
def test_second_workload_agrees_across_engines(mitigate: bool) -> None:
    """The actor-activity spec — different keys, different ops, no join."""
    import os

    import pyarrow.parquet as pq

    from benchmark.runner import _build_engine, _load_config
    from benchmark.storage import s3_filesystem, strip_scheme

    cfg = _load_config(os.environ.get("BENCHMARK_CONFIG", "config/benchmark_config.yaml"))
    cfg["workload"] = load_workload(SHIPPED[1])
    suffix = "on" if mitigate else "off"

    frames = {}
    for engine_name in ["spark", "dask", "ray"]:
        out = f"{cfg['benchmark']['output_path']}/actor-{engine_name}-{suffix}"
        engine = _build_engine(engine_name)
        engine.setup(cfg)
        try:
            result = engine.run(cfg["benchmark"]["input_path"], out, mitigate_skew=mitigate)
        finally:
            engine.teardown()
        assert result.success, f"{engine_name}: {result.error_message}"
        frames[engine_name] = (
            pq.read_table(strip_scheme(out), filesystem=s3_filesystem(cfg["minio"]))
            .to_pandas()
            .sort_values("actor_login", ignore_index=True)
        )

    expected_columns = ["actor_login", "event_count", "repos_touched", "total_commits", "largest_push"]
    reference = frames["spark"]
    assert list(reference.columns) == expected_columns
    assert len(reference) > 0
    for engine_name, frame in frames.items():
        pd.testing.assert_frame_equal(frame[expected_columns], reference[expected_columns],
                                      check_dtype=False, obj=f"actor-activity ({engine_name})")
