# DistributedMind — plan.md (Phase 1)

## Project Goal
Build a benchmark framework comparing Spark, Dask, and Ray on identical data transformation workloads using real GitHub Archive event data, containerized and CI-tested. This phase produces a working, demo-able repo. Skew amplification, fault tolerance, and full observability come in Phase 2 (separate plan).

## Resume Claims This Must Support
- "Distributed computing framework built on Spark, Dask, and Ray to benchmark scalable data transformation across S3-backed Parquet stores with fault-tolerant design."
- "Containerized with Docker for reproducible deployments; implemented observability patterns, CI/CD pipelines, and clean code practices to support resilient, cost-effective systems."

## Tech Stack
- Python 3.11
- PySpark (local mode)
- Dask (LocalCluster)
- Ray Data
- pyarrow / Parquet
- MinIO (S3-compatible, free, Docker)
- Docker + docker-compose
- GitHub Actions (free)
- Basic structured logging (JSON)

## Data Source
**GH Archive** — hourly GitHub event dumps, public, no auth required.
`https://data.gharchive.org/YYYY-MM-DD-H.json.gz` (H = 0-23, no leading zero)

Download 24 hours (one full day) of data. Each hourly file is gzipped JSON (one event per line). During ingestion, filter to event types `PushEvent` and `WatchEvent` only, and flatten each record to a fixed schema:

```
{id, type, actor_login, repo_id, repo_name, created_at, payload_size, payload_action}
```

(`payload_size` populated for PushEvent, `payload_action` populated for WatchEvent — null otherwise). This avoids schema explosion from GH Archive's heterogeneous nested payloads.

## Repo Structure
```
distributedmind/
├── README.md
├── plan.md
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .github/workflows/ci.yml
├── config/benchmark_config.yaml
├── data/
│   └── download_gharchive_data.py
├── engines/
│   ├── __init__.py
│   ├── base.py
│   ├── spark_engine.py
│   ├── dask_engine.py
│   └── ray_engine.py
├── benchmark/
│   ├── __init__.py
│   ├── runner.py
│   ├── metrics.py
│   └── results_writer.py
├── tests/
│   ├── test_engines.py
│   └── test_data_integrity.py
└── results/
    └── .gitkeep
```

## Phase 1 Scope (build this now)

### 1. Data Prep (`data/download_gharchive_data.py`)
- Download N hours of GH Archive data (default: 24 hours = one day, configurable via `--hours` and `--date`)
- For each hourly `.json.gz` file: stream-decompress, parse line-by-line JSON, filter to `type in (PushEvent, WatchEvent)`, flatten to the schema above
- Write flattened records to MinIO bucket `raw-data/`, partitioned by event date (and optionally hour)
- Provide a `--sample-size` flag to cap total rows for fast CI runs (e.g., 50k rows)
- Build a small static repo-metadata lookup table (hardcode ~20-30 well-known repo_ids with `language` and `repo_owner_type` fields) and write it to MinIO as `lookup/repo_metadata.parquet` — used for the join step

### 2. Transformation Workload (identical logic in all 3 engines)
- Read partitioned Parquet from MinIO (`raw-data/`)
- Filter: `type == 'PushEvent'` (commit activity) — or include both event types, document the choice
- Group by `repo_id`, `repo_name`, compute: event count, count of distinct `actor_login` (unique contributors/stargazers), avg `payload_size` (for PushEvent)
- Join with `lookup/repo_metadata.parquet` (repo_id → language, repo_owner_type); repos not in the lookup get `language = "unknown"`
- Write output to MinIO, partitioned by `language`, as Parquet

### 3. Engine Interface (`engines/base.py`)
Abstract class `BenchmarkEngine` with:
- `setup(config)` — initialize engine/cluster
- `run(input_path, output_path) -> BenchmarkResult`
- `teardown()` — cleanup

`BenchmarkResult` dataclass fields: `engine_name`, `duration_seconds`, `rows_processed`, `rows_output`, `peak_memory_mb`, `success`, `error_message`

### 4. Engine Implementations
- `spark_engine.py`: PySpark local mode, S3A connector configured for MinIO
- `dask_engine.py`: `dask.dataframe`, LocalCluster
- `ray_engine.py`: Ray Data, `ray.data.read_parquet`

All three implement the exact same transformation from section 2. Code should be structured so the transformation logic is easy to compare side-by-side (similar method names/ordering across the three files).

### 5. Benchmark Runner (`benchmark/runner.py`)
- CLI: `python -m benchmark.runner --engines spark,dask,ray --output results/run_<timestamp>.json`
- Runs each engine sequentially on the same input, collects `BenchmarkResult`
- Prints comparison table to console (engine, duration, rows/sec, success)
- Writes results to JSON in `results/`

### 6. Docker
- Multi-stage Dockerfile (Python deps + Spark + Java runtime for PySpark)
- `docker-compose.yml`: services for `minio` and `benchmark`, with healthchecks, `depends_on`
- Document exact run sequence in README

### 7. CI/CD (`.github/workflows/ci.yml`)
- Lint with ruff
- Type-check with mypy
- Run `tests/test_engines.py` and `tests/test_data_integrity.py` on the 50k-row sample
- `test_data_integrity.py`: assert all 3 engines produce same row count and matching aggregate values (within floating point tolerance) on identical input
- Build Docker image as a CI step

### 8. README
- Setup instructions: clone, `docker-compose up`, download data, run benchmark
- Architecture diagram (mermaid): data flow from GH Archive → MinIO → 3 engines → results
- Brief note on schema flattening (heterogeneous GH Archive payloads → fixed schema)
- Sample results table from an actual local run
- Note that this is Phase 1; Phase 2 (skew amplification, fault tolerance, full observability) is tracked separately

## Constraints
- Zero cost, zero cloud credentials — MinIO substitutes for S3 entirely
- Full pipeline (download sample data → docker-compose up → run benchmark) completes in under 10 minutes on a laptop
- Clean code: type hints, docstrings, ruff/black formatting throughout
- No hardcoded paths/credentials — everything via `config/benchmark_config.yaml` and env vars (repo-metadata lookup table contents can be a static constant/file, that's fine)

## Definition of Done (Phase 1)
- [ ] `docker-compose up` brings up MinIO + benchmark container successfully
- [ ] Sample GH Archive data downloads, flattens, and lands in MinIO as Parquet
- [ ] Repo-metadata lookup table generated and stored in MinIO
- [ ] All 3 engines run the transformation and produce matching output
- [ ] Benchmark runner produces a comparison table and JSON results file
- [ ] CI passes on a clean clone (lint, type-check, tests, Docker build)
- [ ] README has working setup instructions and a real results table
- [ ] Repo pushed to GitHub, public, linkable from resume/portfolio

## Notes
- Natural skew: even without Phase 2's amplification, GH Archive data will already show meaningful skew in event count and unique actors per repo (popular repos vs. long-tail). Worth a one-line callout in the Phase 1 README — sets up Phase 2 nicely.
- Filter choice: filtering to `PushEvent` only for the transformation keeps the workload focused on commit activity; both event types are ingested to raw-data so Phase 2 can use WatchEvent for skew patterns.
