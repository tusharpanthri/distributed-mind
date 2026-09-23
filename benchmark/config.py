"""YAML config loading with ``${ENV_VAR:default}`` substitution.

Shared by the runner, the data scripts, and the workload-spec loader so they
can't drift apart.
"""

from __future__ import annotations

import os
import re
from typing import Any

_ENV_PATTERN = re.compile(r"\$\{([^}]+)\}")


def resolve_env(value: str) -> str:
    """Expand ``${VAR}`` / ``${VAR:default}`` against the environment."""

    def replace(m: re.Match) -> str:
        var, _, default = m.group(1).partition(":")
        return os.environ.get(var, default)

    return _ENV_PATTERN.sub(replace, value)


def substitute(node: object) -> object:
    """Recursively apply ``resolve_env`` to every string in a parsed document."""
    if isinstance(node, dict):
        return {k: substitute(v) for k, v in node.items()}
    if isinstance(node, list):
        return [substitute(i) for i in node]
    if isinstance(node, str):
        return resolve_env(node)
    return node


def load_yaml_config(config_path: str) -> dict[str, Any]:
    """Load a YAML file, expanding environment placeholders in every string."""
    import yaml

    with open(config_path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return substitute(raw)  # type: ignore[return-value]
