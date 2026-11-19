"""Load monitor configuration."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import yaml

from config_schema import raise_if_invalid, validate_monitors_config


def load_yaml(path: Path) -> Dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def load_monitors_config(path: Path) -> Dict[str, Any]:
    """Load and validate monitors.yaml. Raises ConfigError listing every problem."""
    config = load_yaml(path)
    raise_if_invalid(validate_monitors_config(config), path.name)
    defaults = config.get("defaults") or {}
    databases: List[Dict[str, Any]] = []

    for database in config.get("databases", []):
        if not database.get("enabled", True):
            continue
        merged = {**defaults, **database}
        databases.append(merged)

    return {"defaults": defaults, "databases": databases}
