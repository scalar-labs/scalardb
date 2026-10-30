"""State persistence for deduplication across weekly runs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from models import ComponentState, FeatureState, FeatureUpdate, MonitorState, VersionUpdate, utc_now_iso


def load_state(path: Path) -> MonitorState:
    if not path.exists():
        return MonitorState()

    raw = json.loads(path.read_text(encoding="utf-8"))
    components = {
        key: ComponentState(
            last_notified_version=value["last_notified_version"],
            last_checked=value["last_checked"],
        )
        for key, value in raw.get("components", {}).items()
    }
    features = {
        key: FeatureState(first_seen=value["first_seen"], title=value["title"])
        for key, value in raw.get("features", {}).items()
    }
    return MonitorState(
        last_run=raw.get("last_run"),
        last_run_status=raw.get("last_run_status", "SUCCESS"),
        baseline_established=raw.get("baseline_established", False),
        components=components,
        features=features,
        seen_irrelevant=raw.get("seen_irrelevant", {}),
        source_cursors=raw.get("source_cursors", {}),
        pin_snapshot=raw.get("pin_snapshot", {}),
        errors=raw.get("errors", []),
    )


def save_state(path: Path, state: MonitorState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload: Dict[str, Any] = {
        "last_run": state.last_run,
        "last_run_status": state.last_run_status,
        "baseline_established": state.baseline_established,
        "components": {
            key: {
                "last_notified_version": value.last_notified_version,
                "last_checked": value.last_checked,
            }
            for key, value in state.components.items()
        },
        "features": {
            key: {"first_seen": value.first_seen, "title": value.title}
            for key, value in state.features.items()
        },
        "seen_irrelevant": state.seen_irrelevant,
        "source_cursors": state.source_cursors,
        "pin_snapshot": state.pin_snapshot,
        "errors": state.errors,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def record_component_check(state: MonitorState, update: VersionUpdate) -> None:
    key = update.component_key()
    state.components[key] = ComponentState(
        last_notified_version=update.latest_version,
        last_checked=utc_now_iso(),
    )


def record_feature(state: MonitorState, update: FeatureUpdate) -> None:
    state.features[update.dedup_key()] = FeatureState(
        first_seen=utc_now_iso(),
        title=update.title,
    )


def record_irrelevant(state: MonitorState, update: FeatureUpdate) -> None:
    state.seen_irrelevant[update.dedup_key()] = utc_now_iso()
