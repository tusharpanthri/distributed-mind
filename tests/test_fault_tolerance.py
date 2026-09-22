"""Fault tolerance: injected worker failures are retried with exponential backoff.

Unit tests drive the shared retry loop in ``BenchmarkEngine.run`` with a fake
engine. Integration tests inject a real failure inside a Spark/Dask/Ray worker
task on the skewed dataset and check the job still completes.
"""

from __future__ import annotations

import os
from typing import Any

import pytest

from engines.base import BenchmarkEngine, raise_injected_failure

# ---------------------------------------------------------------------------
# Unit tests (no infrastructure)
# ---------------------------------------------------------------------------


class FakeEngine(BenchmarkEngine):
    name = "fake"

    def __init__(self, fail_times: int = 0) -> None:
        super().__init__()
        self.fail_times = fail_times
        self.attempts: list[bool] = []

    def setup(self, config: dict[str, Any]) -> None:
        self._config = config

    def teardown(self) -> None:
        pass

    def _transform(self, input_path: str, output_path: str, lookup_path: str,
                   mitigate_skew: bool, inject_failure: bool) -> tuple[int, int]:
        self.attempts.append(inject_failure)
        if inject_failure or len(self.attempts) <= self.fail_times:
            raise_injected_failure(self.name)
        return 100, 10


def _config(max_retries: int = 3) -> dict[str, Any]:
    return {
        "benchmark": {"lookup_path": "unused"},
        "fault_tolerance": {"max_retries": max_retries, "backoff_base_seconds": 0.01,
                            "backoff_max_seconds": 0.05},
    }


def test_simulated_failure_is_retried_and_recovers() -> None:
    engine = FakeEngine()
    engine.setup(_config())
    result = engine.run("in", "out", dataset_type="skewed", simulate_failure=True)

    assert result.success
    assert result.retry_count == 1
    assert result.recovery_time_seconds > 0
    assert engine.attempts == [True, False]  # failure injected on the first attempt only
    assert (result.rows_processed, result.rows_output) == (100, 10)


def test_gives_up_after_max_retries() -> None:
    engine = FakeEngine(fail_times=99)
    engine.setup(_config(max_retries=2))
    result = engine.run("in", "out")

    assert not result.success
    assert result.retry_count == 2
    assert len(engine.attempts) == 3
    assert "InjectedWorkerFailure" in result.error_message
    assert result.rows_processed == 0


def test_backoff_is_exponential_and_capped(monkeypatch: pytest.MonkeyPatch) -> None:
    delays: list[float] = []
    monkeypatch.setattr("engines.base.time.sleep", delays.append)
    engine = FakeEngine(fail_times=99)
    engine.setup({"benchmark": {"lookup_path": "x"},
                  "fault_tolerance": {"max_retries": 5, "backoff_base_seconds": 1, "backoff_max_seconds": 5}})
    engine.run("in", "out")

    assert delays == [1, 2, 4, 5, 5]


def test_no_failure_means_no_recovery() -> None:
    engine = FakeEngine()
    engine.setup(_config())
    result = engine.run("in", "out")

    assert result.success
    assert result.retry_count == 0
    assert result.recovery_time_seconds == 0.0


# ---------------------------------------------------------------------------
# Integration tests (real engines against MinIO)
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.skipif(not os.environ.get("MINIO_ENDPOINT"), reason="MINIO_ENDPOINT not set")
@pytest.mark.parametrize("engine_name", ["spark", "dask", "ray"])
def test_engine_recovers_from_worker_failure(engine_name: str) -> None:
    from benchmark.runner import _build_engine, _load_config

    cfg = _load_config(os.environ.get("BENCHMARK_CONFIG", "config/benchmark_config.yaml"))
    engine = _build_engine(engine_name)
    engine.setup(cfg)
    try:
        result = engine.run(
            cfg["benchmark"]["skewed_input_path"],
            f"{cfg['benchmark']['output_path']}/fault-{engine_name}",
            dataset_type="skewed",
            simulate_failure=True,
        )
    finally:
        engine.teardown()

    assert result.success, result.error_message
    assert result.retry_count >= 1
    assert result.recovery_time_seconds > 0
    assert result.rows_processed > 0
