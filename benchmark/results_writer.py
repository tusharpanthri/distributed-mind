"""Serialize benchmark results to JSON and Parquet."""

from __future__ import annotations

import json
import os
from dataclasses import asdict
from datetime import datetime, timezone

import pandas as pd

from engines.base import BenchmarkResult


def write_results(
    results: list[BenchmarkResult],
    output_path: str,
    skew_slowdown: dict[tuple[str, bool], float] | None = None,
) -> None:
    """Write results to ``output_path`` (JSON) and a sibling ``.parquet`` file."""
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    skew_slowdown = skew_slowdown or {}
    rows = []
    for r in results:
        row = asdict(r)
        row["rows_per_second"] = r.rows_per_second
        row["skew_slowdown_ratio"] = (
            skew_slowdown.get((r.engine_name, r.mitigation_applied)) if r.dataset_type == "skewed" else None
        )
        rows.append(row)

    payload = {
        "run_timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "results": rows,
        "skew_slowdown_ratio": [
            {"engine": engine, "mitigation": mitigation, "ratio": ratio}
            for (engine, mitigation), ratio in sorted(skew_slowdown.items())
        ],
    }
    with open(output_path, "w") as f:
        json.dump(payload, f, indent=2, default=str)

    pd.DataFrame(rows).to_parquet(os.path.splitext(output_path)[0] + ".parquet", index=False)
