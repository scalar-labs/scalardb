"""Base plugin interface for feature/API source checkers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

from models import FeatureUpdate


class FeatureSourcePlugin(ABC):
    @abstractmethod
    def plugin_type(self) -> str:
        pass

    @abstractmethod
    def check_features(
        self,
        source_config: Dict[str, Any],
        database: Dict[str, Any],
        cursor: Optional[str],
    ) -> Tuple[List[FeatureUpdate], Optional[str]]:
        """Return feature candidates and an optional new cursor value."""
