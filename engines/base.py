"""Abstract base class and result type shared by all benchmark engines."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


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

    @property
    def rows_per_second(self) -> float:
        if self.duration_seconds <= 0:
            return 0.0
        return self.rows_processed / self.duration_seconds


class BenchmarkEngine(ABC):
    """Contract every engine implementation must satisfy."""

    def __init__(self) -> None:
        self._config: dict[str, Any] = {}

    @abstractmethod
    def setup(self, config: dict[str, Any]) -> None:
        """Initialize the engine/cluster with the provided config."""

    @abstractmethod
    def run(self, input_path: str, output_path: str) -> BenchmarkResult:
        """Execute the transformation workload and return metrics."""

    @abstractmethod
    def teardown(self) -> None:
        """Release engine/cluster resources."""

    def __enter__(self) -> "BenchmarkEngine":
        return self

    def __exit__(self, *_: Any) -> None:
        self.teardown()
