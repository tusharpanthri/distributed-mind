"""Timing/memory capture used by every engine, plus Prometheus benchmark metrics."""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generator

import psutil
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, push_to_gateway, start_http_server

if TYPE_CHECKING:
    from engines.base import BenchmarkResult


@dataclass
class RunMetrics:
    duration_seconds: float = 0.0
    peak_memory_mb: float = 0.0


def _tree_rss_mb(process: psutil.Process) -> float:
    """RSS of this process plus all its children (e.g. PySpark's JVM subprocess)."""
    total = 0
    for proc in [process, *process.children(recursive=True)]:
        try:
            total += proc.memory_info().rss
        except psutil.NoSuchProcess:
            continue
    return total / 1024 / 1024


@contextmanager
def measure(metrics: RunMetrics, sample_interval: float = 0.05) -> Generator[None, None, None]:
    """Record wall-clock time and peak RSS growth over the block.

    RSS includes child processes (PySpark's JVM, Dask workers, Ray workers)
    and is sampled on a background thread, so the reported value is the true
    peak during the run rather than a before/after difference.
    """
    process = psutil.Process()
    baseline_mb = _tree_rss_mb(process)
    peak_mb = baseline_mb
    stop = threading.Event()

    def sample() -> None:
        nonlocal peak_mb
        while not stop.wait(sample_interval):
            peak_mb = max(peak_mb, _tree_rss_mb(process))

    sampler = threading.Thread(target=sample, name="rss-sampler", daemon=True)
    sampler.start()
    start = time.perf_counter()

    try:
        yield
    finally:
        metrics.duration_seconds = time.perf_counter() - start
        stop.set()
        sampler.join()
        peak_mb = max(peak_mb, _tree_rss_mb(process))
        metrics.peak_memory_mb = max(0.0, peak_mb - baseline_mb)


# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------

_DURATION_BUCKETS = (1, 2, 5, 10, 20, 30, 60, 120, 300, 600)


class PrometheusRecorder:
    """Benchmark metrics in a dedicated registry.

    A benchmark run is a batch job that exits before Prometheus's next scrape,
    so metrics are pushed to a Pushgateway (``push``). ``serve`` additionally
    exposes ``/metrics`` over HTTP for the lifetime of the process.
    """

    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        labels = ["engine", "dataset_type", "mitigation"]
        self.job_duration = Histogram(
            "job_duration_seconds", "Wall-clock duration of one benchmark job",
            labels, buckets=_DURATION_BUCKETS, registry=self.registry,
        )
        self.last_duration = Gauge(
            "job_last_duration_seconds", "Duration of the most recent job per configuration",
            labels, registry=self.registry,
        )
        self.rows_processed = Counter(
            "rows_processed", "Input rows processed by successful jobs",
            labels, registry=self.registry,
        )
        self.job_failures = Counter(
            "job_failures", "Jobs that failed after exhausting retries",
            labels, registry=self.registry,
        )
        self.job_retries = Counter(
            "job_retries", "Attempts that failed and were retried",
            labels, registry=self.registry,
        )
        self.recovery_time = Histogram(
            "recovery_time_seconds", "Time from first failure to successful completion",
            ["engine", "dataset_type", "mitigation"], buckets=_DURATION_BUCKETS, registry=self.registry,
        )
        self.last_recovery_time = Gauge(
            "last_recovery_time_seconds", "Recovery time of the most recent recovered job",
            labels, registry=self.registry,
        )
        self.skew_slowdown = Gauge(
            "skew_slowdown_ratio", "Skewed duration / balanced duration for the same engine and mitigation",
            ["engine", "mitigation"], registry=self.registry,
        )
        self.last_run_timestamp = Gauge(
            "benchmark_last_run_timestamp_seconds", "Unix time the last matrix finished",
            registry=self.registry,
        )

    def record(self, result: "BenchmarkResult") -> None:
        labels = {
            "engine": result.engine_name,
            "dataset_type": result.dataset_type,
            "mitigation": "on" if result.mitigation_applied else "off",
        }
        if result.retry_count:
            self.job_retries.labels(**labels).inc(result.retry_count)
        if not result.success:
            self.job_failures.labels(**labels).inc()
            return
        self.job_duration.labels(**labels).observe(result.duration_seconds)
        self.last_duration.labels(**labels).set(result.duration_seconds)
        self.rows_processed.labels(**labels).inc(result.rows_processed)
        if result.recovery_time_seconds > 0:
            self.recovery_time.labels(**labels).observe(result.recovery_time_seconds)
            self.last_recovery_time.labels(**labels).set(result.recovery_time_seconds)

    def set_skew_slowdown(self, engine: str, mitigation: bool, ratio: float) -> None:
        self.skew_slowdown.labels(engine=engine, mitigation="on" if mitigation else "off").set(ratio)

    def serve(self, port: int) -> None:
        start_http_server(port, registry=self.registry)

    def push(self, gateway: str, job: str = "distributedmind_benchmark") -> None:
        self.last_run_timestamp.set_to_current_time()
        push_to_gateway(gateway, job=job, registry=self.registry)
