"""Timing and memory-capture utilities used by all engine implementations."""

from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Generator

import psutil


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
def measure(metrics: RunMetrics) -> Generator[None, None, None]:
    """Context manager that records wall-clock time and peak RSS memory,
    including child processes so JVM-backed engines (PySpark) are captured.
    """
    process = psutil.Process()
    baseline_mb = _tree_rss_mb(process)
    start = time.perf_counter()

    try:
        yield
    finally:
        metrics.duration_seconds = time.perf_counter() - start
        current_mb = _tree_rss_mb(process)
        metrics.peak_memory_mb = max(0.0, current_mb - baseline_mb)
