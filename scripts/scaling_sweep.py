"""Sweep the cluster worker count and report speedup / parallel efficiency.

Runs on the host (it drives `docker compose`), not inside a container. For each
engine and each worker count it brings up only that engine's cluster services,
runs the benchmark against them, then tears them down — so a laptop never hosts
three clusters at once.

    python scripts/scaling_sweep.py                          # 1,2,4 workers, all engines
    python scripts/scaling_sweep.py --workers 1,2 --engines dask,ray
    python scripts/scaling_sweep.py --datasets balanced --repeats 3

Prerequisites: MinIO up with data ingested and amplified, and the benchmark
image built (`docker compose build benchmark`).
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from benchmark.metrics import PrometheusRecorder  # noqa: E402
from benchmark.scaling import format_scaling_table, scaling_points  # noqa: E402
from engines.base import BenchmarkResult  # noqa: E402

COMPOSE = ["docker", "compose", "-f", "docker-compose.yml", "-f", "docker-compose.cluster.yml"]

# Services each engine needs: (always-on, scaled-per-worker-count)
ENGINE_SERVICES: dict[str, tuple[str, str]] = {
    "spark": ("spark-master", "spark-worker"),
    "dask": ("dask-scheduler", "dask-worker"),
    "ray": ("ray-head", "ray-worker"),
}


def _run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess:
    print(f"$ {' '.join(cmd)}", flush=True)
    return subprocess.run(cmd, cwd=REPO_ROOT, **kwargs)  # type: ignore[call-overload]


def start_cluster(engine: str, workers: int, worker_cores: int) -> None:
    head, worker = ENGINE_SERVICES[engine]
    _run([*COMPOSE, "up", "-d", "--scale", f"{worker}={workers}", head, worker],
         check=True, env=_env(workers, worker_cores))
    _wait_for_workers(engine, workers)


def stop_cluster(engine: str) -> None:
    head, worker = ENGINE_SERVICES[engine]
    _run([*COMPOSE, "rm", "-sf", head, worker], check=False,
         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _env(workers: int, worker_cores: int) -> dict[str, str]:
    import os

    env = dict(os.environ)
    env["CLUSTER_WORKERS"] = str(workers)
    env["CLUSTER_WORKER_CORES"] = str(worker_cores)
    return env


def _wait_for_workers(engine: str, workers: int, timeout: float = 180.0) -> None:
    """Block until the expected number of workers has registered."""
    _, worker_service = ENGINE_SERVICES[engine]
    deadline = time.time() + timeout
    running = 0
    while time.time() < deadline:
        result = _run([*COMPOSE, "ps", "-q", worker_service],
                      check=False, capture_output=True, text=True)
        running = len([line for line in result.stdout.splitlines() if line.strip()])
        if running >= workers:
            # Containers are up; give the workers a moment to register with
            # their scheduler (engines also wait/retry on connect).
            time.sleep(8)
            return
        time.sleep(3)
    raise TimeoutError(f"{engine}: only saw {running} of {workers} workers before timeout")


def run_benchmark(engine: str, workers: int, worker_cores: int, datasets: str,
                  repeats: int, out_json: Path) -> list[BenchmarkResult]:
    container_out = f"results/{out_json.name}"
    _run([*COMPOSE, "run", "--rm", "benchmark",
          "-m", "benchmark.runner",
          "--engines", engine,
          "--datasets", datasets,
          "--mitigation", "off",
          "--repeats", str(repeats),
          "--cluster-workers", str(workers),
          "--output", container_out],
         check=True, env=_env(workers, worker_cores))

    payload = json.loads((REPO_ROOT / container_out).read_text())
    fields = {f for f in BenchmarkResult.__dataclass_fields__}
    return [BenchmarkResult(**{k: v for k, v in row.items() if k in fields})
            for row in payload["results"]]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engines", default="spark,dask,ray")
    parser.add_argument("--workers", default="1,2,4", help="Worker counts to sweep")
    parser.add_argument("--worker-cores", type=int, default=2)
    parser.add_argument("--datasets", default="balanced,skewed")
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--keep-up", action="store_true", help="Don't tear the cluster down at the end")
    parser.add_argument("--pushgateway", default="localhost:9091",
                        help="Pushgateway for scaling metrics ('' to skip)")
    args = parser.parse_args()

    engines = [e.strip() for e in args.engines.split(",") if e.strip()]
    worker_counts = [int(w) for w in args.workers.split(",") if w.strip()]
    stamp = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    all_results: list[BenchmarkResult] = []
    for engine in engines:
        for workers in worker_counts:
            print(f"\n=== {engine}: {workers} worker(s) x {args.worker_cores} cores ===", flush=True)
            start_cluster(engine, workers, args.worker_cores)
            try:
                out = Path(f"results/scaling_{stamp}_{engine}_{workers}w.json")
                all_results.extend(
                    run_benchmark(engine, workers, args.worker_cores, args.datasets, args.repeats, out)
                )
            finally:
                if not args.keep_up:
                    stop_cluster(engine)

    points = scaling_points(all_results)
    print("\n" + format_scaling_table(points) + "\n")

    if args.pushgateway:
        recorder = PrometheusRecorder()
        for r in all_results:
            recorder.record(r)
        for p in points:
            recorder.record_scaling(p)
        try:
            recorder.push(args.pushgateway, job="distributedmind_scaling")
            print(f"Pushed scaling metrics to {args.pushgateway}")
        except OSError as exc:
            print(f"Metrics push failed ({exc}); continuing")

    summary = REPO_ROOT / f"results/scaling_{stamp}.json"
    summary.write_text(json.dumps({
        "run_timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "worker_cores": args.worker_cores,
        "results": [asdict(r) for r in all_results],
        "scaling": [asdict(p) for p in points],
    }, indent=2))
    print(f"Wrote {summary}")

    return 0 if all(r.success for r in all_results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
