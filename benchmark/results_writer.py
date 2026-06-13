"""Serialize benchmark results to JSON."""

from __future__ import annotations

import json
import os
from dataclasses import asdict
from datetime import datetime, timezone

from engines.base import BenchmarkResult


def write_results(results: list[BenchmarkResult], output_path: str) -> None:
    """Write a list of BenchmarkResult objects to a JSON file."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    payload = {
        "run_timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "results": [asdict(r) for r in results],
    }
    with open(output_path, "w") as f:
        json.dump(payload, f, indent=2, default=str)
