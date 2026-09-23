"""Speedup and parallel efficiency across worker counts.

Pure functions over ``BenchmarkResult`` lists, so the maths is testable without
a cluster. ``scripts/scaling_sweep.py`` collects the results these consume.
"""

from __future__ import annotations

from dataclasses import dataclass

from engines.base import BenchmarkResult


@dataclass
class ScalingPoint:
    """One (engine, dataset, worker count) measurement relative to the baseline."""

    engine_name: str
    dataset_type: str
    worker_count: int
    total_cores: int
    duration_seconds: float
    rows_per_second: float
    speedup: float               # baseline duration / this duration
    parallel_efficiency: float   # speedup / (workers / baseline workers)

    @property
    def is_baseline(self) -> bool:
        return self.speedup == 1.0 and self.parallel_efficiency == 1.0


def _key(r: BenchmarkResult) -> tuple[str, str]:
    return (r.engine_name, r.dataset_type)


def baseline_of(results: list[BenchmarkResult]) -> BenchmarkResult | None:
    """The successful run with the fewest workers (the scaling reference)."""
    usable = [r for r in results if r.success and r.duration_seconds > 0 and r.worker_count > 0]
    return min(usable, key=lambda r: r.worker_count) if usable else None


def speedup(baseline: BenchmarkResult, result: BenchmarkResult) -> float:
    """How many times faster than the baseline; 0.0 when it can't be computed."""
    if not (baseline.success and result.success) or result.duration_seconds <= 0:
        return 0.0
    return baseline.duration_seconds / result.duration_seconds


def parallel_efficiency(baseline: BenchmarkResult, result: BenchmarkResult) -> float:
    """Speedup ÷ how many times more workers. 1.0 is linear scaling."""
    if baseline.worker_count <= 0 or result.worker_count <= 0:
        return 0.0
    factor = result.worker_count / baseline.worker_count
    if factor <= 0:
        return 0.0
    return speedup(baseline, result) / factor


def scaling_points(results: list[BenchmarkResult]) -> list[ScalingPoint]:
    """Build one point per successful run, each compared to its own group's baseline.

    Results are grouped by (engine, dataset) so a Spark balanced run is only
    ever compared against Spark balanced at the smallest worker count.
    """
    groups: dict[tuple[str, str], list[BenchmarkResult]] = {}
    for r in results:
        groups.setdefault(_key(r), []).append(r)

    points: list[ScalingPoint] = []
    for (engine, dataset), group in groups.items():
        base = baseline_of(group)
        if base is None:
            continue
        for r in sorted(group, key=lambda x: x.worker_count):
            if not r.success:
                continue
            points.append(
                ScalingPoint(
                    engine_name=engine,
                    dataset_type=dataset,
                    worker_count=r.worker_count,
                    total_cores=r.total_cores,
                    duration_seconds=r.duration_seconds,
                    rows_per_second=r.rows_per_second,
                    speedup=speedup(base, r),
                    parallel_efficiency=parallel_efficiency(base, r),
                )
            )
    return points


def format_scaling_table(points: list[ScalingPoint]) -> str:
    """Console table: one row per measurement, grouped by engine and dataset."""
    header = (
        f"{'Engine':<7} {'Dataset':<9} {'Workers':>7} {'Cores':>6} {'Duration (s)':>12} "
        f"{'Rows/sec':>10} {'Speedup':>8} {'Efficiency':>11}"
    )
    lines = ["=" * len(header), header, "-" * len(header)]
    for p in sorted(points, key=lambda x: (x.engine_name, x.dataset_type, x.worker_count)):
        lines.append(
            f"{p.engine_name:<7} {p.dataset_type:<9} {p.worker_count:>7} {p.total_cores:>6} "
            f"{p.duration_seconds:>12.2f} {p.rows_per_second:>10,.0f} "
            f"{p.speedup:>7.2f}x {p.parallel_efficiency:>10.0%}"
        )
    lines.append("=" * len(header))
    return "\n".join(lines)
