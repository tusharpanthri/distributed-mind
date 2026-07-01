"""Benchmark runner — orchestrates all three engines and prints a comparison table.

Usage:
    python -m benchmark.runner --engines spark,dask,ray
    python -m benchmark.runner --engines dask,ray --output results/run_custom.json
"""

from __future__ import annotations

import logging
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any

import click
import yaml
from pythonjsonlogger import jsonlogger

from benchmark.results_writer import write_results
from engines.base import BenchmarkEngine, BenchmarkResult

logger = logging.getLogger("distributedmind.runner")


def _setup_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(jsonlogger.JsonFormatter("%(asctime)s %(name)s %(levelname)s %(message)s"))
    logging.root.setLevel(logging.INFO)
    logging.root.addHandler(handler)


def _resolve_env(value: str) -> str:
    def replace(m: re.Match) -> str:
        var, _, default = m.group(1).partition(":")
        return os.environ.get(var, default)

    return re.sub(r"\$\{([^}]+)\}", replace, value)


def _load_config(config_path: str) -> dict[str, Any]:
    with open(config_path) as f:
        raw = yaml.safe_load(f)

    def walk(node: object) -> object:
        if isinstance(node, dict):
            return {k: walk(v) for k, v in node.items()}
        if isinstance(node, list):
            return [walk(i) for i in node]
        if isinstance(node, str):
            return _resolve_env(node)
        return node

    return walk(raw)  # type: ignore[return-value]


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


def _print_table(results: list[BenchmarkResult]) -> None:
    header = f"{'Engine':<10} {'Duration (s)':>14} {'Rows/sec':>12} {'Rows In':>10} {'Rows Out':>10} {'Mem (MB)':>10} {'OK':>4}"
    print("\n" + "=" * len(header))
    print(header)
    print("-" * len(header))
    for r in results:
        status = "OK" if r.success else "FAIL"
        rps = f"{r.rows_per_second:,.0f}" if r.success else "n/a"
        print(
            f"{r.engine_name:<10} "
            f"{r.duration_seconds:>14.2f} "
            f"{rps:>12} "
            f"{r.rows_processed:>10,} "
            f"{r.rows_output:>10,} "
            f"{r.peak_memory_mb:>10.1f} "
            f"{status:>4}"
        )
    print("=" * len(header) + "\n")


def run_benchmark(
    engine_names: list[str],
    config: dict[str, Any],
    output_path: str,
) -> list[BenchmarkResult]:
    input_path = config["benchmark"]["input_path"]
    output_base = config["benchmark"]["output_path"]
    results: list[BenchmarkResult] = []

    for name in engine_names:
        logger.info("Starting engine", extra={"engine": name})
        engine_output = f"{output_base}/{name}"
        engine = _build_engine(name)
        try:
            engine.setup(config)
            result = engine.run(input_path, engine_output)
        except Exception as exc:
            result = BenchmarkResult(
                engine_name=name,
                duration_seconds=0.0,
                rows_processed=0,
                rows_output=0,
                peak_memory_mb=0.0,
                success=False,
                error_message=str(exc),
            )
        finally:
            try:
                engine.teardown()
            except Exception:
                pass

        results.append(result)
        logger.info(
            "Engine finished",
            extra={
                "engine": name,
                "success": result.success,
                "duration_seconds": result.duration_seconds,
            },
        )

    _print_table(results)
    write_results(results, output_path)
    logger.info("Results written", extra={"path": output_path})
    return results


@click.command()
@click.option(
    "--engines",
    default="spark,dask,ray",
    show_default=True,
    help="Comma-separated list of engines to run",
)
@click.option(
    "--output",
    default=None,
    help="Path to output JSON (default: results/run_<timestamp>.json)",
)
@click.option(
    "--config",
    "config_path",
    default="config/benchmark_config.yaml",
    show_default=True,
)
def main(engines: str, output: str | None, config_path: str) -> None:
    """Run the distributed benchmark across selected engines."""
    _setup_logging()
    config = _load_config(config_path)

    if output is None:
        ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output = f"results/run_{ts}.json"

    engine_names = [e.strip() for e in engines.split(",") if e.strip()]
    run_benchmark(engine_names, config, output)


if __name__ == "__main__":
    main()
