# DistributedMind — plan_phase4.md (Phase 4)

## Prerequisite
Phases 1–3 are complete and pushed: three engines running identical
transformations, skew amplification and mitigation, fault injection with
retry/recovery, Prometheus + Grafana observability, and multi-node clusters
with a worker-count scaling sweep.

## Goal
The harness was reusable but the **workload was not**. The GH Archive query was
hardcoded across all three engines (~71 references to `PushEvent`, `repo_id`,
`actor_login`, `payload_size`, `language`), so benchmarking a different dataset
meant editing `_transform` in three files and keeping them equivalent by hand —
the very thing the project otherwise proves with tests.

Phase 4 makes the workload a **declarative YAML spec** the engines translate,
validates a user's dataset against it with actionable errors, and documents the
**data contract** so someone can bring their own Parquet.

## Scope

### 1. The spec (`workloads/spec.py`, `config/workloads/*.yaml`)
Columns and their types, an optional filter, group keys, aggregations, an
optional lookup join, and an output partition column. `--workload` selects one;
`benchmark.workload` in the config is the default.

Supported ops are exactly those the mitigation paths can decompose:

| Op | Partial (per key + salt) | Combine |
|---|---|---|
| count | count | sum |
| sum | sum | sum |
| mean | sum + non-null count | sum/sum |
| min / max | min / max | min / max |
| distinct_count | — | de-duplicate (keys, column), then count |

Anything else is rejected when the spec loads, naming the supported ops. This
parameterizes the existing mitigation strategies rather than redesigning them.

### 2. Engines translate the spec
Each engine's `_transform` becomes: filter → group-by/aggregate (direct or
decomposed) → optional join → write partitioned. The three implementations stay
readable side by side, as `plan.md` originally required.

### 3. Validation before anything runs
The runner reads the dataset schema and checks every referenced column and type
up front, reporting **all** problems at once with the column named, as a clean
CLI error rather than a traceback from inside a Spark stage.

### 4. Skew amplification follows the spec
`data/amplify_skew.py` uses `spec.filter`, `spec.skew_key` and `spec.row_id`
instead of hardcoded GH Archive columns, so skew amplification works for a
user's dataset too.

### 5. Shared helpers
`benchmark/config.py` (env-substituting YAML loader, previously duplicated in
the runner and the downloader) and `benchmark/storage.py` (S3 filesystem and
schema reading, previously duplicated in the amplifier and the Ray engine).

## Known limit
**Ray's skew mitigation supports at most one `distinct_count`.** Spreading a hot
key across buckets relies on that column's value sets being disjoint per bucket,
which doesn't hold for two distinct columns at once. The engine raises a clear
error instead of silently over-counting; Spark and Dask have no such limit.

## Constraints
- The default spec must reproduce the existing published numbers exactly —
  572,369 output rows with matching aggregates.
- All Phase 1–3 behaviour, tests and CI stay green.

## Definition of Done (Phase 4)
- [x] Workload is a YAML spec; engines contain no dataset-specific columns
- [x] Two specs ship, and a test runs the second across all three engines
- [x] Datasets validated up front with every problem named
- [x] Skew amplification is spec-driven
- [x] `tests/test_workload_spec.py` covers parsing, validation and decomposition
- [x] Existing unit + integration tests still pass unchanged
- [x] README documents the data contract, supported ops and measured data shape
- [x] 24h matrix re-measured; an A/B against pre-refactor code showed the
      differences were run-order/cache effects, not the refactor
- [x] CI green, including cluster smoke
