"""Timing and memory-capture utilities used by all engine implementations."""

from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Generator

import psutil


@dataclass
class RunMetrics:
    duration_seconds: float = 0.0
    peak_memory_mb: float = 0.0


@contextmanager
def measure(metrics: RunMetrics) -> Generator[None, None, None]:
    """Context manager that records wall-clock time and peak RSS memory."""
    process = psutil.Process()
    baseline_mb = process.memory_info().rss / 1024 / 1024
    peak_mb = baseline_mb
    start = time.perf_counter()

    try:
        yield
    finally:
        metrics.duration_seconds = time.perf_counter() - start
        current_mb = process.memory_info().rss / 1024 / 1024
        peak_mb = max(peak_mb, current_mb)
        metrics.peak_memory_mb = peak_mb - baseline_mb
