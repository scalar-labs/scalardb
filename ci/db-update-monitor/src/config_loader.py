"""Load monitor configuration."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import yaml


def load_yaml(path: Path) -> Dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def load_monitors_config(path: Path) -> Dict[str, Any]:
    config = load_yaml(path)
    defaults = config.get("defaults", {})
    databases: List[Dict[str, Any]] = []

    for database in config.get("databases", []):
        if not database.get("enabled", True):
            continue
        merged = {**defaults, **database}
        databases.append(merged)

    return {"defaults": defaults, "databases": databases}
