"""Base plugin interface for upstream version checkers."""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from models import VersionUpdate


def resolve_granularity(
    component: Dict[str, Any],
    upstream_config: Dict[str, Any],
    database: Dict[str, Any],
    default: str,
) -> str:
    """Most specific setting wins: component, upstream, database (incl. defaults), plugin."""
    return (
        component.get("version_granularity")
        or upstream_config.get("version_granularity")
        or database.get("version_granularity")
        or default
    )


def filter_by_version_pattern(versions: List[str], upstream_config: Dict[str, Any]) -> List[str]:
    """Keep versions matching `version_pattern`, e.g. only the jre8 builds of mssql-jdbc."""
    pattern = upstream_config.get("version_pattern")
    if not pattern:
        return versions
    regex = re.compile(pattern)
    return [version for version in versions if regex.search(version)]


class SourcePlugin(ABC):
    @abstractmethod
    def plugin_type(self) -> str:
        pass

    @abstractmethod
    def check(
        self,
        upstream_config: Dict[str, Any],
        database: Dict[str, Any],
        component: Dict[str, Any],
        current_pin: str,
    ) -> Optional[VersionUpdate]:
        pass
