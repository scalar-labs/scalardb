"""Data models for the database update monitor."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional


class ComponentType(str, Enum):
    SDK = "sdk"
    JDBC_DRIVER = "jdbc-driver"
    SERVER = "server"
    CONNECTOR = "connector"
    DRIVER = "driver"


class UpdateLevel(str, Enum):
    MAJOR = "major_update"
    MINOR = "minor_update"
    PATCH = "patch_update"
    UNKNOWN = "update"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass
class VersionUpdate:
    database_id: str
    database_name: str
    component_id: str
    component_type: ComponentType
    source_type: str
    current_version: str
    latest_version: str
    new_versions: List[str]
    update_level: UpdateLevel
    url: Optional[str] = None
    adapter_package: Optional[str] = None
    detected_at: str = field(default_factory=utc_now_iso)

    def component_key(self) -> str:
        return f"{self.database_id}:{self.component_id}"

    def dedup_key(self) -> str:
        return f"{self.component_key()}:{self.latest_version}"


@dataclass
class FeatureUpdate:
    database_id: str
    database_name: str
    title: str
    description: str
    source_type: str
    matched_keywords: List[str]
    relevant: bool = False
    relevance_reason: str = ""
    url: Optional[str] = None
    adapter_capabilities: Optional[Dict[str, str]] = None
    published_at: Optional[str] = None
    include_all: bool = False
    version: Optional[str] = None
    detected_at: str = field(default_factory=utc_now_iso)

    def dedup_key(self) -> str:
        canonical = self.url or f"{self.title}:{self.published_at or ''}"
        return f"{self.database_id}:{self.source_type}:{canonical}"


@dataclass
class ComponentState:
    last_notified_version: str
    last_checked: str


@dataclass
class FeatureState:
    first_seen: str
    title: str


@dataclass
class MonitorState:
    last_run: Optional[str] = None
    last_run_status: str = "SUCCESS"
    baseline_established: bool = False
    components: Dict[str, ComponentState] = field(default_factory=dict)
    features: Dict[str, FeatureState] = field(default_factory=dict)
    seen_irrelevant: Dict[str, str] = field(default_factory=dict)
    source_cursors: Dict[str, str] = field(default_factory=dict)
    pin_snapshot: Dict[str, str] = field(default_factory=dict)
    errors: List[Dict[str, str]] = field(default_factory=list)
