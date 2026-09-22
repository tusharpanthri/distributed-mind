"""Abstract base class and result type shared by all benchmark engines."""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Literal

from benchmark.metrics import RunMetrics, measure

DatasetType = Literal["balanced", "skewed"]

logger = logging.getLogger("distributedmind.engine")


class InjectedWorkerFailure(RuntimeError):
    """Raised inside a worker task when failure simulation is enabled."""


def raise_injected_failure(engine_name: str) -> None:
    """Called from inside a distributed task (Spark UDF, Dask partition, Ray batch)."""
    raise InjectedWorkerFailure(f"simulated worker failure in {engine_name} task")


@dataclass
class BenchmarkResult:
    """Captures the outcome and performance metrics of one engine run."""

    engine_name: str
    duration_seconds: float
    rows_processed: int
    rows_output: int
    peak_memory_mb: float
    success: bool
    error_message: str = ""
    dataset_type: str = "balanced"
    mitigation_applied: bool = False
    retry_count: int = 0
    recovery_time_seconds: float = 0.0

    @property
    def rows_per_second(self) -> float:
        if self.duration_seconds <= 0:
            return 0.0
        return self.rows_processed / self.duration_seconds


class BenchmarkEngine(ABC):
    """Contract every engine implementation must satisfy.

    Subclasses implement lifecycle (``setup``/``teardown``) and ``_transform``;
    ``run`` wraps the transform with timing, failure injection, and
    retry-with-exponential-backoff so all three engines recover identically.
    """

    name: str = "base"

    def __init__(self) -> None:
        self._config: dict[str, Any] = {}

    @abstractmethod
    def setup(self, config: dict[str, Any]) -> None:
        """Initialize the engine/cluster with the provided config."""

    @abstractmethod
    def teardown(self) -> None:
        """Release engine/cluster resources."""

    @abstractmethod
    def _transform(
        self,
        input_path: str,
        output_path: str,
        lookup_path: str,
        mitigate_skew: bool,
        inject_failure: bool,
    ) -> tuple[int, int]:
        """Run the workload once; return (rows_processed, rows_output).

        When ``inject_failure`` is set, a task running on a worker (not the
        driver) must raise ``InjectedWorkerFailure`` partway through the job.
        """

    def run(
        self,
        input_path: str,
        output_path: str,
        dataset_type: DatasetType = "balanced",
        mitigate_skew: bool = False,
        simulate_failure: bool = False,
    ) -> BenchmarkResult:
        """Execute the workload, retrying failed attempts with exponential backoff."""
        ft = self._config.get("fault_tolerance", {})
        max_retries = int(ft.get("max_retries", 3))
        backoff_base = float(ft.get("backoff_base_seconds", 0.5))
        backoff_max = float(ft.get("backoff_max_seconds", 8.0))
        lookup_path = self._config["benchmark"]["lookup_path"]

        metrics = RunMetrics()
        retry_count = 0
        first_failure_at: float | None = None
        rows_processed = rows_output = 0
        error = ""
        success = False

        with measure(metrics):
            for attempt in range(max_retries + 1):
                try:
                    rows_processed, rows_output = self._transform(
                        input_path,
                        output_path,
                        lookup_path,
                        mitigate_skew,
                        inject_failure=simulate_failure and attempt == 0,
                    )
                    success = True
                    break
                except Exception as exc:  # engines wrap worker errors in their own types
                    error = f"{type(exc).__name__}: {exc}"
                    if first_failure_at is None:
                        first_failure_at = time.perf_counter()
                    if attempt == max_retries:
                        logger.exception("Run failed after retries", extra={"engine": self.name})
                        break
                    retry_count += 1
                    delay = min(backoff_max, backoff_base * 2**attempt)
                    logger.warning(
                        "Attempt failed, retrying",
                        extra={"engine": self.name, "attempt": attempt + 1,
                               "backoff_seconds": delay, "error": error[:300]},
                    )
                    time.sleep(delay)

        recovery = 0.0
        if success and first_failure_at is not None:
            recovery = time.perf_counter() - first_failure_at

        return BenchmarkResult(
            engine_name=self.name,
            duration_seconds=metrics.duration_seconds,
            rows_processed=rows_processed if success else 0,
            rows_output=rows_output if success else 0,
            peak_memory_mb=metrics.peak_memory_mb,
            success=success,
            error_message="" if success else error,
            dataset_type=dataset_type,
            mitigation_applied=mitigate_skew,
            retry_count=retry_count,
            recovery_time_seconds=recovery,
        )

    def __enter__(self) -> "BenchmarkEngine":
        return self

    def __exit__(self, *_: Any) -> None:
        self.teardown()
