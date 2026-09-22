"""Make the repo importable from engine worker processes, not just the test process.

pytest's ``pythonpath`` setting only edits this process's sys.path. Spark
Python workers, Dask worker processes, and Ray workers unpickle functions
from ``engines.*`` and need the repo root on PYTHONPATH as well.
"""

import os

_ROOT = os.path.dirname(os.path.abspath(__file__))
_paths = [p for p in os.environ.get("PYTHONPATH", "").split(os.pathsep) if p]
if _ROOT not in _paths:
    os.environ["PYTHONPATH"] = os.pathsep.join([_ROOT, *_paths])
