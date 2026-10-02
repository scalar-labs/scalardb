"""Base plugin interface for upstream version checkers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from models import VersionUpdate


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
