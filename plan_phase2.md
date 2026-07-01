# DistributedMind — plan_phase2.md (Phase 2)

## Prerequisite
Phase 1 is complete, verified, and pushed: 3 engines (Spark, Dask, Ray) running identical transformations on GH Archive data via MinIO, benchmark runner producing comparison tables, Docker + CI all working. Before starting, read this file plus the current repo structure (`README.md`, `engines/`, `benchmark/`, `config/benchmark_config.yaml`) to orient.

## Goal
Add skew amplification, per-engine skew mitigation, fault-tolerance testing, and full observability (Prometheus + Grafana). Turns the project from "ran the same job on 3 engines" into "measured how each engine degrades under skew and what mitigation buys you" — the actual interview talking point.

## New/Changed Structure
```
distributedmind/
├── (existing Phase 1 structure)
├── data/
│   └── amplify_skew.py             # NEW
├── engines/
│   ├── base.py                     # MODIFIED: add mitigate_skew, dataset_type params
│   ├── spark_engine.py             # MODIFIED: add salting logic
│   ├── dask_engine.py              # MODIFIED: add repartition logic
│   └── ray_engine.py               # MODIFIED: add custom partitioning
├── benchmark/
│   ├── runner.py                   # MODIFIED: run full matrix
│   └── metrics.py                  # MODIFIED: add Prometheus metrics
├── observability/
│   ├── prometheus.yml              # NEW
│   └── grafana/dashboard.json      # NEW
├── docker-compose.yml              # MODIFIED: add prometheus, grafana services
└── tests/
    ├── test_fault_tolerance.py     # NEW
    └── test_skew.py                # NEW
```

## Scope

### 1. Skew Amplification (`data/amplify_skew.py`)
- Read the GH Archive dataset from MinIO (`raw-data/`)
- Identify the natural distribution of `event_count` per `repo_id` (already skewed — top repos vs long tail)
- Create an amplified-skew copy in `raw-data-skewed/`: take the top N repos by event count and replicate their rows by a configurable `--skew-factor` (default 10x)
- Both the original (`balanced`) and amplified (`skewed`) datasets remain available for benchmarking
- Log the resulting skew ratio (top repo's share of total events, before and after amplification) for the README

### 2. Skew Mitigation Per Engine
Each engine's `run()` gains parameters: `dataset_type: Literal["balanced", "skewed"]`, `mitigate_skew: bool`

- **Spark**: implement salting on the `groupBy(repo_id)` key — split hot keys into N sub-keys with a random salt, aggregate at sub-key level, then combine results back per `repo_id`
- **Dask**: use `repartition()` informed by the known key distribution before the groupby, so hot `repo_id`s don't all land in one partition
- **Ray**: implement custom block partitioning in `ray.data` so blocks containing hot `repo_id`s are split across more workers than blocks of long-tail data

`BenchmarkResult` gains fields: `dataset_type`, `mitigation_applied`

### 3. Fault Tolerance (`tests/test_fault_tolerance.py` + engine changes)
- Add `--simulate-failure` flag to runner: on the skewed run, inject a worker process kill or forced exception partway through execution
- Each engine catches the failure, retries with exponential backoff (configurable max retries via `config/benchmark_config.yaml`)
- `BenchmarkResult` gains `recovery_time_seconds` and `retry_count`
- Test asserts: simulated failure → job still completes successfully → `recovery_time_seconds` and `retry_count` are non-zero and logged

### 4. Benchmark Matrix (`benchmark/runner.py` rewrite)
Full matrix: 3 engines × 2 datasets (balanced/skewed) × 2 modes (mitigated/unmitigated) = 12 runs.
- On the balanced dataset, mitigation should have negligible effect — running both confirms mitigation logic doesn't *hurt* when unneeded (worth keeping for the README's "mitigation isn't free" point)
- Output comparison table: engine, dataset_type, mitigation, duration, throughput, **skew_slowdown_ratio** (skewed duration ÷ balanced duration for same engine/mitigation setting)
- Write full results to JSON and Parquet in `results/`

### 5. Observability
- `benchmark/metrics.py`: Prometheus client, expose `/metrics` during runs
  - `job_duration_seconds` histogram, labeled by `engine`, `dataset_type`, `mitigation`
  - `rows_processed_total` counter
  - `job_failures_total` counter
  - `skew_slowdown_ratio` gauge, labeled by `engine`, `mitigation`
  - `recovery_time_seconds` histogram
- `observability/prometheus.yml`: scrape config for the benchmark app's `/metrics` endpoint
- `observability/grafana/dashboard.json`: panels for
  - Duration grouped bar chart (engine × dataset_type × mitigation)
  - Skew slowdown ratio (with vs without mitigation, per engine)
  - Failure/recovery timeline
- `docker-compose.yml`: add `prometheus` and `grafana` services with healthchecks, Grafana auto-provisioned to load the dashboard JSON on startup

### 6. README Updates
- Update architecture diagram (mermaid) to include skew amplification step and Prometheus/Grafana
- New results table: full 12-run matrix with real numbers from a local run
- New section "Skew Mitigation Techniques": explain salting (Spark), repartitioning (Dask), custom partitioning (Ray) — what each does, why, and what the measured slowdown reduction was
- New section "Fault Tolerance": describe failure injection, retry behavior, observed recovery times
- New section "Cost Implications": map measured CPU/memory/duration to theoretical AWS costs (EMR for Spark, EC2 for Dask/Ray) — framed as production cost modeling discussion, no real cloud spend

## Constraints
- Still zero cost, zero cloud credentials, runs via `docker-compose up`
- Full 12-run matrix on the sample dataset completes in under 20 minutes on a laptop
- Don't break Phase 1 — existing tests (`test_engines.py`, `test_data_integrity.py`) must still pass

## Definition of Done (Phase 2)
- [ ] `amplify_skew.py` produces a skewed dataset in MinIO with a logged before/after skew ratio
- [ ] All 3 engines support `dataset_type` and `mitigate_skew` params with real, non-trivial mitigation logic
- [ ] Fault injection + retry/recovery implemented and tested (`test_fault_tolerance.py` passes)
- [ ] `test_skew.py` validates skewed dataset has expected distribution and mitigated runs reduce `skew_slowdown_ratio`
- [ ] Full 12-run benchmark matrix executes and produces comparison table with `skew_slowdown_ratio`
- [ ] Prometheus + Grafana running via `docker-compose up`, dashboard displays real data from a run
- [ ] README updated: new architecture diagram, results table, mitigation/fault-tolerance/cost sections
- [ ] CI extended to cover new tests on sample data, still green
- [ ] Pushed to GitHub
