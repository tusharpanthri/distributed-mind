# DistributedMind — plan_phase3.md (Phase 3)

## Prerequisite
Phases 1 and 2 are complete and pushed: three engines running identical
transformations, skew amplification and per-engine mitigation, fault injection
with retry/recovery, and Prometheus + Grafana observability.

## Goal
Everything up to now ran **single-node local mode** (`local[4]`, a Dask
`LocalCluster`, a local `ray.init()`). Phase 3 runs the same workloads against
**real clusters** — Spark standalone, a Dask scheduler + workers, and a Ray head
+ workers, each as its own container — and measures how each engine *scales* as
worker count goes 1 → 2 → 4.

The interview point this unlocks: not "which engine is fastest on my laptop" but
"how much of each extra worker actually turns into throughput, and where does
each scheduler stop paying for itself".

## Scope

### 1. Cluster wiring (`docker-compose.cluster.yml`)
An overlay on the Phase 1/2 compose file, so local mode stays the default and
the existing quick start is untouched.

| Engine | Services | Driver connects via |
|---|---|---|
| Spark | `spark-master`, `spark-worker` (scaled) | `spark://spark-master:7077`, client mode |
| Dask | `dask-scheduler`, `dask-worker` (scaled) | `Client("tcp://dask-scheduler:8786")` |
| Ray | `ray-head` (0 CPUs), `ray-worker` (scaled) | driver joins the cluster as a 0-CPU node |

Every service reuses the benchmark image. Each worker gets an identical budget
(2 cores / 2 GB, enforced with compose `cpus`/`mem_limit`) so engines stay
comparable, and only the worker count varies.

### 2. Engine changes
`setup()` in each engine branches on a configured address: connect to the
cluster, else build the local cluster exactly as before. Config gains
`spark.master` / `dask.scheduler_address` / `ray.address`, all env-overridable
and all defaulting to local mode.

Cluster runs must **fail fast rather than hang**: a job submitted to a cluster
with no live workers otherwise queues forever. Each engine waits for workers to
register (Spark executors, Dask workers, Ray CPUs) and raises on timeout.

Parallelism is derived from the *live* cluster rather than local config: Dask's
partition count from registered worker threads, Ray's bucket count from
`ray.cluster_resources()`, Spark's shuffle partitions from the core budget.

### 3. Scaling measurement
- `BenchmarkResult` gains `worker_count` and `total_cores`.
- `benchmark/scaling.py` holds the maths as pure functions — `speedup`,
  `parallel_efficiency`, `scaling_points` — grouped per (engine, dataset) and
  measured against that group's smallest successful worker count.
- `scripts/scaling_sweep.py` (host-side) brings up **only** the engine under
  test at each worker count, runs the benchmark, tears it down, then prints a
  speedup/efficiency table, writes `results/scaling_<ts>.json`, and pushes
  metrics.

### 4. Observability
New `cluster_speedup_ratio` and `parallel_efficiency_ratio` gauges, a `workers`
label on the existing metrics, and two Grafana panels (speedup, efficiency).
Scaling runs push under their own Pushgateway job so they don't overwrite the
local matrix.

### 5. Tests and CI
- `tests/test_scaling.py`: speedup/efficiency maths including degenerate cases,
  grouping, and config resolution (local by default, cluster when env is set).
- New `cluster-smoke` CI job: brings up all three clusters with one worker each,
  ingests the 50k sample, runs one job per engine against the real clusters, and
  asserts all three succeed with matching row counts. Because the engines fail
  fast, this genuinely proves the drivers reached their schedulers.

### 6. README
New **Scaling** section with the measured speedup/efficiency table and what it
says about each engine, plus cluster quick-start commands.

## Constraints
- Local mode remains the default; all Phase 1/2 commands, tests, and CI keep
  working unchanged.
- Data grows to a **full 24h GH Archive day** (~3.5M PushEvents) so jobs are long
  enough that worker count dominates startup overhead.
- Equal core budget per worker across engines, or the comparison is meaningless.

## Definition of Done (Phase 3)
- [x] `docker-compose.cluster.yml` brings up all three clusters, workers register
- [x] Each engine runs against its cluster and produces output identical to local mode
- [x] Worker-count sweep (1, 2, 4) completes for all three engines
- [x] Speedup and parallel efficiency reported per engine and dataset
- [x] Grafana shows the scaling panels with real data
- [x] `tests/test_scaling.py` passes; existing unit + integration tests still green
- [x] `cluster-smoke` CI job green alongside the existing jobs
- [x] README scaling section written from a real sweep
