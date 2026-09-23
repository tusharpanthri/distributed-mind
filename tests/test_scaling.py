"""Scaling maths and cluster-vs-local config resolution (no cluster required)."""

from __future__ import annotations

import pytest

from benchmark.scaling import (
    baseline_of,
    format_scaling_table,
    parallel_efficiency,
    scaling_points,
    speedup,
)
from engines.base import BenchmarkResult


def _result(engine: str = "dask", dataset: str = "balanced", workers: int = 1,
            duration: float = 10.0, success: bool = True, cores_per_worker: int = 2) -> BenchmarkResult:
    return BenchmarkResult(
        engine_name=engine, duration_seconds=duration, rows_processed=1_000_000,
        rows_output=100, peak_memory_mb=0.0, success=success, dataset_type=dataset,
        worker_count=workers, total_cores=workers * cores_per_worker,
    )


# ---------------------------------------------------------------------------
# speedup / efficiency
# ---------------------------------------------------------------------------

def test_perfect_scaling_is_linear() -> None:
    base = _result(workers=1, duration=10.0)
    doubled = _result(workers=2, duration=5.0)

    assert speedup(base, doubled) == pytest.approx(2.0)
    assert parallel_efficiency(base, doubled) == pytest.approx(1.0)


def test_sublinear_scaling() -> None:
    base = _result(workers=1, duration=10.0)
    four = _result(workers=4, duration=4.0)  # 2.5x faster on 4x the workers

    assert speedup(base, four) == pytest.approx(2.5)
    assert parallel_efficiency(base, four) == pytest.approx(0.625)


def test_scaling_can_go_backwards() -> None:
    base = _result(workers=1, duration=10.0)
    slower = _result(workers=4, duration=12.0)  # more workers, more coordination

    assert speedup(base, slower) == pytest.approx(10 / 12)
    assert parallel_efficiency(base, slower) < 0.25


def test_baseline_against_itself_is_one() -> None:
    base = _result(workers=2, duration=7.0)
    assert speedup(base, base) == pytest.approx(1.0)
    assert parallel_efficiency(base, base) == pytest.approx(1.0)


@pytest.mark.parametrize("bad", [
    _result(success=False, workers=2, duration=5.0),
    _result(workers=2, duration=0.0),
])
def test_unusable_results_give_zero(bad: BenchmarkResult) -> None:
    assert speedup(_result(workers=1, duration=10.0), bad) == 0.0


def test_efficiency_needs_worker_counts() -> None:
    local = _result(workers=0, duration=10.0)   # local mode carries no worker count
    assert parallel_efficiency(local, _result(workers=2, duration=5.0)) == 0.0
    assert parallel_efficiency(_result(workers=1), local) == 0.0


# ---------------------------------------------------------------------------
# grouping
# ---------------------------------------------------------------------------

def test_baseline_is_the_smallest_successful_worker_count() -> None:
    results = [_result(workers=4), _result(workers=1, success=False), _result(workers=2)]
    base = baseline_of(results)
    assert base is not None and base.worker_count == 2  # the 1-worker run failed


def test_points_are_grouped_per_engine_and_dataset() -> None:
    results = [
        _result("spark", "balanced", workers=1, duration=20.0),
        _result("spark", "balanced", workers=2, duration=10.0),
        _result("spark", "skewed", workers=1, duration=40.0),
        _result("spark", "skewed", workers=2, duration=30.0),
        _result("ray", "balanced", workers=1, duration=8.0),
        _result("ray", "balanced", workers=2, duration=8.0),
    ]
    points = {(p.engine_name, p.dataset_type, p.worker_count): p for p in scaling_points(results)}

    assert points[("spark", "balanced", 2)].speedup == pytest.approx(2.0)
    assert points[("spark", "skewed", 2)].speedup == pytest.approx(40 / 30)
    assert points[("ray", "balanced", 2)].speedup == pytest.approx(1.0)      # no gain
    assert points[("ray", "balanced", 2)].parallel_efficiency == pytest.approx(0.5)
    assert points[("spark", "balanced", 1)].is_baseline


def test_failed_runs_are_excluded_but_dont_break_the_group() -> None:
    results = [_result(workers=1, duration=10.0), _result(workers=2, success=False)]
    points = scaling_points(results)
    assert [p.worker_count for p in points] == [1]


def test_group_without_a_baseline_is_skipped() -> None:
    assert scaling_points([_result(workers=0, duration=5.0)]) == []


def test_table_renders_every_point() -> None:
    table = format_scaling_table(scaling_points([
        _result("dask", "balanced", workers=1, duration=10.0),
        _result("dask", "balanced", workers=2, duration=6.0),
    ]))
    assert "Efficiency" in table
    assert "1.67x" in table  # 10 / 6
    assert table.count("dask") == 2


# ---------------------------------------------------------------------------
# config resolution: local by default, cluster when the env says so
# ---------------------------------------------------------------------------

def test_config_defaults_to_local_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    from benchmark.runner import _load_config

    for var in ("SPARK_MASTER_URL", "DASK_SCHEDULER_ADDRESS", "RAY_ADDRESS", "CLUSTER_WORKERS"):
        monkeypatch.delenv(var, raising=False)
    cfg = _load_config("config/benchmark_config.yaml")

    assert cfg["spark"]["master"] == "local[4]"
    assert cfg["dask"]["scheduler_address"] == ""
    assert cfg["ray"]["address"] == ""
    assert int(cfg["cluster"]["workers"]) == 0


def test_config_picks_up_cluster_addresses(monkeypatch: pytest.MonkeyPatch) -> None:
    from benchmark.runner import _load_config

    monkeypatch.setenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    monkeypatch.setenv("DASK_SCHEDULER_ADDRESS", "tcp://dask-scheduler:8786")
    monkeypatch.setenv("RAY_ADDRESS", "ray://ray-head:10001")
    monkeypatch.setenv("CLUSTER_WORKERS", "4")
    cfg = _load_config("config/benchmark_config.yaml")

    assert cfg["spark"]["master"].startswith("spark://")
    assert cfg["dask"]["scheduler_address"].startswith("tcp://")
    assert cfg["ray"]["address"].startswith("ray://")
    assert int(cfg["cluster"]["workers"]) == 4
