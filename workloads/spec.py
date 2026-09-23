"""Declarative workload specs.

A spec describes the benchmark workload as data — which columns it needs, how
to filter, what to group by, which aggregates to compute, an optional lookup
join, and how to partition the output. All three engines translate the same
spec into their own APIs, so they cannot drift apart, and running the benchmark
on a different dataset means writing YAML rather than editing three engines.

Supported aggregations are exactly those the skew-mitigation paths can
decompose into a partial aggregate plus a combine step:

    count | sum | mean | min | max     additive over a salt
    distinct_count                     de-duplicate (keys + column), then count
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa

from benchmark.config import load_yaml_config

#: op -> how a partial aggregate is combined across salts/buckets.
#: ``mean`` is special: it is carried as a sum and a non-null count.
_COMBINE = {"count": "sum", "sum": "sum", "min": "min", "max": "max"}
SUPPORTED_OPS = (*_COMBINE, "mean", "distinct_count")

#: Declared dtype -> the pyarrow types accepted for it. Deliberately
#: family-based: a real dataset shouldn't be rejected because a column is
#: int32 where the spec says int64.
_TYPE_FAMILIES: dict[str, tuple[Any, ...]] = {
    "string": (pa.types.is_string, pa.types.is_large_string),
    "int64": (pa.types.is_integer,),
    "int32": (pa.types.is_integer,),
    "float64": (pa.types.is_floating, pa.types.is_integer),
    "bool": (pa.types.is_boolean,),
    "timestamp": (pa.types.is_timestamp,),
}


class WorkloadSpecError(ValueError):
    """The spec itself is invalid (bad op, unknown column reference, ...)."""


class WorkloadSchemaError(ValueError):
    """A dataset doesn't satisfy the spec's declared columns."""


@dataclass(frozen=True)
class PartialField:
    """One intermediate column produced during a decomposed aggregation."""

    name: str
    op: str
    column: str | None

    @property
    def value_column(self) -> str:
        if self.column is None:
            raise WorkloadSpecError(f"partial field {self.name!r} has no column")
        return self.column


@dataclass(frozen=True)
class Aggregation:
    name: str
    op: str
    column: str | None = None

    @property
    def is_distinct(self) -> bool:
        return self.op == "distinct_count"

    @property
    def value_column(self) -> str:
        """The column this aggregation reads (every op except ``count`` has one)."""
        if self.column is None:
            raise WorkloadSpecError(f"aggregation {self.name!r} ({self.op}) has no column")
        return self.column

    def partial_fields(self) -> list[PartialField]:
        """Intermediate fields for the salted/bucketed path.

        Empty for ``distinct_count``, which is not additive over a salt and is
        computed by de-duplicating (group keys + column) instead.
        """
        if self.is_distinct:
            return []
        if self.op == "mean":
            return [
                PartialField(f"{self.name}__sum", "sum", self.column),
                PartialField(f"{self.name}__count", "count", self.column),
            ]
        return [PartialField(f"{self.name}__{self.op}", self.op, self.column)]

    @property
    def combine_op(self) -> str:
        """How this aggregate's partials are combined (``mean`` is sum/sum)."""
        if self.op == "mean":
            return "mean"
        if self.is_distinct:
            raise WorkloadSpecError("distinct_count is combined by de-duplication, not an op")
        return _COMBINE[self.op]


@dataclass(frozen=True)
class FilterSpec:
    column: str
    equals: Any


@dataclass(frozen=True)
class JoinSpec:
    path: str
    key: str            # named 'key', not 'on': YAML 1.1 parses a bare `on:` as True
    columns: list[str]
    missing: str = "unknown"


@dataclass(frozen=True)
class WorkloadSpec:
    name: str
    columns: dict[str, str]
    group_by: list[str]
    aggregations: list[Aggregation]
    row_id: str
    skew_key: str
    filter: FilterSpec | None = None
    require_not_null: list[str] = field(default_factory=list)
    join: JoinSpec | None = None
    partition_by: str | None = None

    # ------------------------------------------------------------------
    # Convenience views used by the engines
    # ------------------------------------------------------------------

    @property
    def distinct_aggregations(self) -> list[Aggregation]:
        return [a for a in self.aggregations if a.is_distinct]

    @property
    def additive_aggregations(self) -> list[Aggregation]:
        return [a for a in self.aggregations if not a.is_distinct]

    @property
    def input_columns(self) -> list[str]:
        """Columns to read from the dataset (everything the spec references)."""
        needed = {self.row_id, self.skew_key, *self.group_by, *self.require_not_null}
        if self.filter:
            needed.add(self.filter.column)
        needed.update(a.column for a in self.aggregations if a.column)
        return [c for c in self.columns if c in needed]

    @property
    def output_columns(self) -> list[str]:
        cols = [*self.group_by, *(a.name for a in self.aggregations)]
        if self.join:
            cols += [c for c in self.join.columns if c not in cols]
        return cols

    def validate_against(self, schema: pa.Schema, *, source: str = "dataset") -> None:
        """Check a real dataset satisfies the declared columns and dtypes.

        Reports every problem at once, so a mismatched dataset is fixed in one
        pass rather than one error per run.
        """
        problems: list[str] = []
        present = {f.name: f.type for f in schema}
        for column in self.input_columns:
            declared = self.columns[column]
            actual = present.get(column)
            if actual is None:
                problems.append(f"  - missing column {column!r} (declared {declared})")
                continue
            checks = _TYPE_FAMILIES.get(declared)
            if checks and not any(check(actual) for check in checks):
                problems.append(
                    f"  - column {column!r} is {actual}, spec declares {declared}"
                )
        if problems:
            raise WorkloadSchemaError(
                f"{source} does not match workload {self.name!r}:\n"
                + "\n".join(problems)
                + f"\n  available columns: {', '.join(sorted(present)) or '(none)'}"
            )


def _parse_aggregation(raw: dict[str, Any], columns: dict[str, str]) -> Aggregation:
    try:
        name, op = raw["name"], raw["op"]
    except KeyError as exc:
        raise WorkloadSpecError(f"aggregation is missing {exc} in {raw!r}") from exc
    if op not in SUPPORTED_OPS:
        raise WorkloadSpecError(
            f"aggregation {name!r} uses unsupported op {op!r}; supported: {', '.join(sorted(SUPPORTED_OPS))}"
        )
    column = raw.get("column")
    if op == "count":
        column = None
    elif not column:
        raise WorkloadSpecError(f"aggregation {name!r} with op {op!r} needs a 'column'")
    elif column not in columns:
        raise WorkloadSpecError(f"aggregation {name!r} references undeclared column {column!r}")
    return Aggregation(name=name, op=op, column=column)


def parse_workload(raw: dict[str, Any]) -> WorkloadSpec:
    """Build a validated spec from a parsed YAML document."""
    for required in ("name", "columns", "group_by", "aggregations", "row_id", "skew_key"):
        if required not in raw:
            raise WorkloadSpecError(f"workload spec is missing required field {required!r}")

    columns: dict[str, str] = dict(raw["columns"])
    unknown_types = {t for t in columns.values() if t not in _TYPE_FAMILIES}
    if unknown_types:
        raise WorkloadSpecError(
            f"unsupported column type(s) {sorted(unknown_types)}; "
            f"supported: {', '.join(sorted(_TYPE_FAMILIES))}"
        )

    aggregations = [_parse_aggregation(a, columns) for a in raw["aggregations"]]
    group_by = list(raw["group_by"])

    filter_spec = None
    if raw.get("filter"):
        filter_spec = FilterSpec(column=raw["filter"]["column"], equals=raw["filter"]["equals"])

    join_spec = None
    if raw.get("join"):
        j = raw["join"]
        if "on" in j or True in j:
            raise WorkloadSpecError(
                "join uses 'key', not 'on' (YAML parses a bare `on:` as the boolean True)"
            )
        join_spec = JoinSpec(path=j["path"], key=j["key"], columns=list(j["columns"]),
                             missing=j.get("missing", "unknown"))

    spec = WorkloadSpec(
        name=raw["name"],
        columns=columns,
        group_by=group_by,
        aggregations=aggregations,
        row_id=raw["row_id"],
        skew_key=raw["skew_key"],
        filter=filter_spec,
        require_not_null=list(raw.get("require_not_null", [])),
        join=join_spec,
        partition_by=raw.get("partition_by"),
    )
    _check_references(spec)
    return spec


def _check_references(spec: WorkloadSpec) -> None:
    """Catch spec-internal mistakes before any engine touches data."""
    problems = []
    for label, names in (
        ("group_by", spec.group_by),
        ("require_not_null", spec.require_not_null),
        ("row_id", [spec.row_id]),
        ("skew_key", [spec.skew_key]),
        ("filter.column", [spec.filter.column] if spec.filter else []),
    ):
        for name in names:
            if name not in spec.columns:
                problems.append(f"{label} references undeclared column {name!r}")

    if not spec.group_by:
        problems.append("group_by must name at least one column")
    if not spec.aggregations:
        problems.append("aggregations must contain at least one entry")

    names = [a.name for a in spec.aggregations]
    duplicates = {n for n in names if names.count(n) > 1}
    if duplicates:
        problems.append(f"duplicate aggregation name(s) {sorted(duplicates)}")
    clashes = set(names) & set(spec.group_by)
    if clashes:
        problems.append(f"aggregation name(s) {sorted(clashes)} collide with group_by columns")

    if spec.join and spec.join.key not in spec.group_by:
        problems.append(
            f"join.key {spec.join.key!r} must be one of the group_by columns "
            f"({', '.join(spec.group_by)}), since the join happens after aggregation"
        )
    if spec.partition_by:
        available = set(spec.output_columns)
        if spec.partition_by not in available:
            problems.append(
                f"partition_by {spec.partition_by!r} is not an output column "
                f"({', '.join(sorted(available))})"
            )
    if problems:
        raise WorkloadSpecError(
            f"invalid workload spec {spec.name!r}:\n" + "\n".join(f"  - {p}" for p in problems)
        )


def load_workload(path: str) -> WorkloadSpec:
    """Load and validate a workload spec from a YAML file."""
    return parse_workload(load_yaml_config(path))
