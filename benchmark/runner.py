"""Benchmark runner — runs the engine × dataset × mitigation matrix and compares results.

Usage:
    python -m benchmark.runner                                   # full 12-run matrix
    python -m benchmark.runner --engines dask,ray --datasets balanced --mitigation off
    python -m benchmark.runner --simulate-failure --repeats 3
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from typing import Any

import click
from pythonjsonlogger import jsonlogger

from benchmark.config import load_yaml_config
from benchmark.metrics import PrometheusRecorder
from benchmark.results_writer import write_results
from benchmark.storage import dataset_schema
from engines.base import BenchmarkEngine, BenchmarkResult, DatasetType
from workloads.spec import WorkloadSchemaError, WorkloadSpec, WorkloadSpecError, load_workload

logger = logging.getLogger("distributedmind.runner")


def _setup_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(jsonlogger.JsonFormatter("%(asctime)s %(name)s %(levelname)s %(message)s"))
    logging.root.setLevel(logging.INFO)
    logging.root.addHandler(handler)


#: Kept as the module-level name the tests and data scripts already import.
_load_config = load_yaml_config


def load_workload_spec(config: dict[str, Any], workload_path: str | None) -> WorkloadSpec:
    """Load the workload spec named on the CLI, else the one in the config."""
    path = workload_path or config["benchmark"]["workload"]
    spec = load_workload(path)
    logger.info("Workload loaded", extra={"workload": spec.name, "path": path})
    return spec


def validate_datasets(config: dict[str, Any], spec: WorkloadSpec, datasets: list[DatasetType]) -> None:
    """Fail fast if the data doesn't match the spec.

    Checked once up front so a mismatched column is reported in seconds with
    the column named, rather than surfacing deep inside a Spark stage.
    """
    bench = config["benchmark"]
    paths = {"balanced": bench["input_path"], "skewed": bench["skewed_input_path"]}
    for dataset in datasets:
        spec.validate_against(dataset_schema(paths[dataset], config["minio"]),
                              source=f"{dataset} dataset ({paths[dataset]})")
    if spec.join:
        join_schema = dataset_schema(spec.join.path, config["minio"])
        missing = [c for c in (spec.join.key, *spec.join.columns) if c not in join_schema.names]
        if missing:
            raise WorkloadSchemaError(
                f"lookup table ({spec.join.path}) is missing column(s) {missing}; "
                f"it has: {', '.join(join_schema.names)}"
            )
    logger.info("Dataset schema validated", extra={"workload": spec.name})


def _build_engine(name: str) -> BenchmarkEngine:
    if name == "spark":
        from engines.spark_engine import SparkEngine
        return SparkEngine()
    if name == "dask":
        from engines.dask_engine import DaskEngine
        return DaskEngine()
    if name == "ray":
        from engines.ray_engine import RayEngine
        return RayEngine()
    raise ValueError(f"Unknown engine: {name!r}")


def _config_key(r: BenchmarkResult) -> tuple[str, str, bool]:
    return (r.engine_name, r.dataset_type, r.mitigation_applied)


def skew_slowdown_ratios(results: list[BenchmarkResult]) -> dict[tuple[str, bool], float]:
    """Skewed duration ÷ balanced duration for each (engine, mitigation) pair."""
    by_key = {_config_key(r): r for r in results if r.success}
    ratios: dict[tuple[str, bool], float] = {}
    for (engine, dataset, mitigation), r in by_key.items():
        balanced = by_key.get((engine, "balanced", mitigation))
        if dataset == "skewed" and balanced and balanced.duration_seconds > 0:
            ratios[(engine, mitigation)] = r.duration_seconds / balanced.duration_seconds
    return ratios


def _print_table(results: list[BenchmarkResult], ratios: dict[tuple[str, bool], float]) -> None:
    header = (
        f"{'Engine':<7} {'Dataset':<9} {'Mitig.':<6} {'Wrk':>4} {'Duration (s)':>12} {'Rows/sec':>10} "
        f"{'Rows In':>9} {'Rows Out':>9} {'Mem (MB)':>9} {'Skew x':>7} {'Retries':>7} {'Recov (s)':>9} {'OK':>4}"
    )
    print("\n" + "=" * len(header))
    print(header)
    print("-" * len(header))
    for r in results:
        status = "OK" if r.success else "FAIL"
        rps = f"{r.rows_per_second:,.0f}" if r.success else "n/a"
        ratio = ratios.get((r.engine_name, r.mitigation_applied)) if r.dataset_type == "skewed" else None
        ratio_text = f"{ratio:.2f}" if ratio else "-"
        print(
            f"{r.engine_name:<7} "
            f"{r.dataset_type:<9} "
            f"{'on' if r.mitigation_applied else 'off':<6} "
            f"{(r.worker_count or '-'):>4} "
            f"{r.duration_seconds:>12.2f} "
            f"{rps:>10} "
            f"{r.rows_processed:>9,} "
            f"{r.rows_output:>9,} "
            f"{r.peak_memory_mb:>9.1f} "
            f"{ratio_text:>7} "
            f"{r.retry_count:>7} "
            f"{r.recovery_time_seconds:>9.2f} "
            f"{status:>4}"
        )
    print("=" * len(header) + "\n")


def _median_result(runs: list[BenchmarkResult]) -> BenchmarkResult:
    """Pick the run with the median duration, preferring successful runs."""
    successes = sorted((r for r in runs if r.success), key=lambda r: r.duration_seconds)
    pool = successes or runs
    return pool[(len(pool) - 1) // 2]


def run_benchmark(
    engine_names: list[str],
    config: dict[str, Any],
    output_path: str,
    datasets: list[DatasetType] | None = None,
    mitigations: list[bool] | None = None,
    repeats: int = 1,
    warmup: bool = True,
    simulate_failure: bool = False,
    recorder: PrometheusRecorder | None = None,
) -> list[BenchmarkResult]:
    """Run every engine × dataset × mitigation combination and report the results.

    Each engine is set up once and reused across its combinations; an untimed
    warmup run first absorbs one-off costs (JVM/JIT, jar resolution, worker
    spawn) so the first measured configuration isn't penalised.
    """
    datasets = datasets or ["balanced"]
    mitigations = mitigations or [False]
    bench = config["benchmark"]
    input_paths = {"balanced": bench["input_path"], "skewed": bench["skewed_input_path"]}
    output_base = bench["output_path"]
    results: list[BenchmarkResult] = []

    for name in engine_names:
        logger.info("Starting engine", extra={"engine": name})
        engine_output = f"{output_base}/{name}"
        engine = _build_engine(name)
        engine_results: list[BenchmarkResult] = []
        try:
            engine.setup(config)
            if warmup:
                engine.run(input_paths[datasets[0]], engine_output)
            for dataset in datasets:
                for mitigate in mitigations:
                    inject = simulate_failure and dataset == "skewed"
                    runs = [
                        engine.run(input_paths[dataset], engine_output, dataset_type=dataset,
                                   mitigate_skew=mitigate, simulate_failure=inject)
                        for _ in range(repeats)
                    ]
                    result = _median_result(runs)
                    engine_results.append(result)
                    logger.info(
                        "Run finished",
                        extra={"engine": name, "dataset_type": dataset, "mitigation": mitigate,
                               "success": result.success, "duration_seconds": result.duration_seconds,
                               "retry_count": result.retry_count},
                    )
        except Exception as exc:
            logger.exception("Engine setup failed", extra={"engine": name})
            done = {_config_key(r) for r in engine_results}
            engine_results.extend(
                BenchmarkResult(
                    engine_name=name, duration_seconds=0.0, rows_processed=0, rows_output=0,
                    peak_memory_mb=0.0, success=False, error_message=str(exc),
                    dataset_type=dataset, mitigation_applied=mitigate,
                )
                for dataset in datasets
                for mitigate in mitigations
                if (name, dataset, mitigate) not in done
            )
        finally:
            try:
                engine.teardown()
            except Exception:
                logger.exception("Engine teardown failed", extra={"engine": name})
        results.extend(engine_results)

    ratios = skew_slowdown_ratios(results)
    _print_table(results, ratios)
    write_results(results, output_path, ratios)
    logger.info("Results written", extra={"path": output_path})

    if recorder:
        for result in results:
            recorder.record(result)
        for (engine_name, mitigation), ratio in ratios.items():
            recorder.set_skew_slowdown(engine_name, mitigation, ratio)
        gateway = config.get("observability", {}).get("pushgateway", "")
        if gateway:
            try:
                # Separate job so injected-failure runs (whose durations include
                # retries + backoff) don't overwrite the clean timing matrix.
                job = "distributedmind_fault_injection" if simulate_failure else "distributedmind_benchmark"
                recorder.push(gateway, job=job)
                logger.info("Metrics pushed", extra={"pushgateway": gateway})
            except OSError as exc:
                logger.warning("Metrics push failed", extra={"pushgateway": gateway, "error": str(exc)})
    return results


def _parse_list(value: str, allowed: dict[str, Any]) -> list[Any]:
    items = [v.strip() for v in value.split(",") if v.strip()]
    unknown = [v for v in items if v not in allowed]
    if unknown:
        raise click.BadParameter(f"unknown value(s) {unknown}; choose from {sorted(allowed)}")
    return [allowed[v] for v in items]


@click.command()
@click.option(
    "--engines",
    default="spark,dask,ray",
    show_default=True,
    help="Comma-separated list of engines to run",
)
@click.option("--datasets", default="balanced,skewed", show_default=True,
              help="Comma-separated: balanced, skewed")
@click.option("--mitigation", default="off,on", show_default=True,
              help="Comma-separated: off, on (skew mitigation)")
@click.option("--repeats", default=1, show_default=True, help="Runs per configuration; the median is reported")
@click.option("--warmup/--no-warmup", default=True, show_default=True,
              help="Untimed run per engine before measuring")
@click.option("--simulate-failure", is_flag=True,
              help="Inject a worker-task failure into each skewed run to exercise retry/recovery")
@click.option("--workload", "workload_path", default=None,
              help="Workload spec YAML (default: benchmark.workload from the config)")
@click.option("--cluster-workers", default=None, type=int,
              help="Worker count to record on results (default: cluster.workers from config)")
@click.option("--metrics-port", default=0, show_default=True,
              help="Also serve /metrics on this port while running (0 = off)")
@click.option(
    "--output",
    default=None,
    help="Path to output JSON (default: results/run_<timestamp>.json; a .parquet is written alongside)",
)
@click.option(
    "--config",
    "config_path",
    default="config/benchmark_config.yaml",
    show_default=True,
)
def main(
    engines: str,
    datasets: str,
    mitigation: str,
    repeats: int,
    warmup: bool,
    simulate_failure: bool,
    workload_path: str | None,
    cluster_workers: int | None,
    metrics_port: int,
    output: str | None,
    config_path: str,
) -> None:
    """Run the distributed benchmark matrix across selected engines."""
    _setup_logging()
    config = _load_config(config_path)
    if cluster_workers is not None:
        config.setdefault("cluster", {})["workers"] = cluster_workers

    if output is None:
        ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output = f"results/run_{ts}.json"

    recorder = PrometheusRecorder()
    if metrics_port:
        recorder.serve(metrics_port)

    engine_names = [e.strip() for e in engines.split(",") if e.strip()]
    dataset_list = _parse_list(datasets, {"balanced": "balanced", "skewed": "skewed"})

    # A bad spec or mismatched dataset is a user error, not a crash: show the
    # message without a traceback.
    try:
        config["workload"] = load_workload_spec(config, workload_path)
        validate_datasets(config, config["workload"], dataset_list)
    except (WorkloadSpecError, WorkloadSchemaError) as exc:
        raise click.ClickException(str(exc)) from exc

    run_benchmark(
        engine_names,
        config,
        output,
        datasets=dataset_list,
        mitigations=_parse_list(mitigation, {"off": False, "on": True}),
        repeats=max(1, repeats),
        warmup=warmup,
        simulate_failure=simulate_failure,
        recorder=recorder,
    )


if __name__ == "__main__":
    main()
