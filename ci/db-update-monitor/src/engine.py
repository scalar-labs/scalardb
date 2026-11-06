"""Monitor engine orchestrator."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from config_loader import load_monitors_config
from diff import apply_notifications, establish_baseline, partition_feature_updates, partition_updates
from models import FeatureUpdate, MonitorState, VersionUpdate, utc_now_iso
from pin_reader import PinReader
from plugins import get_feature_plugin, get_plugin
from relevance import evaluate_relevance, load_relevance_rules
from state import load_state, save_state, record_feature, record_irrelevant
from text_util import feature_cursor_key, resolve_cursor

logger = logging.getLogger(__name__)


class MonitorEngine:
    def __init__(
        self,
        config_path: Path,
        state_path: Path,
        repo_root: Path,
        database_filter: Optional[str] = None,
        relevance_rules_path: Optional[Path] = None,
    ):
        self.config_path = config_path
        self.state_path = state_path
        self.repo_root = repo_root
        self.database_filter = database_filter
        self.relevance_rules_path = relevance_rules_path or config_path.parent / "relevance-rules.yaml"

    def run(self, dry_run: bool = False) -> Dict[str, Any]:
        monitors_config = load_monitors_config(self.config_path)
        state = load_state(self.state_path)
        pin_reader = PinReader(self.repo_root)
        relevance_rules = load_relevance_rules(self.relevance_rules_path)

        databases = monitors_config["databases"]
        defaults = monitors_config.get("defaults", {})
        first_run_policy = defaults.get("first_run_policy", "baseline_silent")
        if self.database_filter:
            databases = [db for db in databases if db["id"] == self.database_filter]
            if not state.baseline_established:
                logger.warning(
                    "Running with --filter before the first full run; "
                    "the next unfiltered run will still establish the baseline silently."
                )

        if first_run_policy != "baseline_silent" and not state.baseline_established:
            state.baseline_established = True

        all_version_updates: List[VersionUpdate] = []
        all_feature_updates: List[FeatureUpdate] = []
        errors: List[Dict[str, str]] = []

        for database in databases:
            version_updates, version_errors = self._check_version_components(database, pin_reader)
            all_version_updates.extend(version_updates)
            errors.extend(version_errors)

            feature_updates, feature_errors, cursor_updates = self._check_feature_sources(
                database, state, relevance_rules
            )
            all_feature_updates.extend(feature_updates)
            errors.extend(feature_errors)
            state.source_cursors.update(cursor_updates)

        all_feature_updates = self._drop_version_only_features(
            all_version_updates, all_feature_updates, databases
        )

        version_to_notify, version_skipped = partition_updates(state, all_version_updates)
        feature_to_notify, feature_skipped = partition_feature_updates(state, all_feature_updates)
        if not self.database_filter:
            establish_baseline(state)

        run_status = self._resolve_status(errors, databases)

        report = {
            "last_run": utc_now_iso(),
            "last_run_status": run_status,
            "baseline_established": state.baseline_established,
            "checked_databases": len(databases),
            "detected_version_updates": len(all_version_updates),
            "detected_feature_updates": len(all_feature_updates),
            "new_version_updates": len(version_to_notify),
            "new_feature_updates": len(feature_to_notify),
            "skipped_version_updates": len(version_skipped),
            "skipped_feature_updates": len(feature_skipped),
            "errors": errors,
            "detected_versions": [self._version_to_dict(update) for update in all_version_updates],
            "detected_features": [self._feature_to_dict(update) for update in all_feature_updates],
            "version_updates": [self._version_to_dict(update) for update in version_to_notify],
            "feature_updates": [self._feature_to_dict(update) for update in feature_to_notify],
        }

        if not dry_run:
            apply_notifications(state, version_to_notify, feature_to_notify)
            state.last_run = report["last_run"]
            state.last_run_status = run_status
            state.errors = errors
            state.pin_snapshot.update(pin_reader.build_pin_snapshot(databases))
            save_state(self.state_path, state)

        report["notify_version_updates"] = version_to_notify
        report["notify_feature_updates"] = feature_to_notify
        return report

    def _check_version_components(
        self,
        database: Dict[str, Any],
        pin_reader: PinReader,
    ) -> Tuple[List[VersionUpdate], List[Dict[str, str]]]:
        updates: List[VersionUpdate] = []
        errors: List[Dict[str, str]] = []

        for component in database.get("components", []):
            upstream = component.get("upstream", {})
            source_type = upstream.get("type")
            try:
                current_pin = pin_reader.resolve(component["current_version"])
                plugin = get_plugin(source_type)
                result = plugin.check(upstream, database, component, current_pin)
                if result:
                    updates.append(result)
            except Exception as exc:
                logger.exception(
                    "Version check failed for %s/%s", database["id"], component.get("id")
                )
                errors.append(
                    {
                        "database_id": database["id"],
                        "component_id": component.get("id", ""),
                        "source_type": source_type or "unknown",
                        "error": str(exc),
                        "timestamp": utc_now_iso(),
                    }
                )

        return updates, errors

    def _check_feature_sources(
        self,
        database: Dict[str, Any],
        state: MonitorState,
        relevance_rules: Dict[str, List[str]],
    ) -> Tuple[List[FeatureUpdate], List[Dict[str, str]], Dict[str, str]]:
        updates: List[FeatureUpdate] = []
        errors: List[Dict[str, str]] = []
        cursor_updates: Dict[str, str] = {}

        for source in database.get("sources", []):
            source_type = source.get("type")
            cursor_key = feature_cursor_key(database["id"], source)
            cursor = resolve_cursor(state.source_cursors, cursor_key, database["id"], source_type or "")
            silent_baseline = cursor is None and state.baseline_established
            try:
                plugin = get_feature_plugin(source_type)
                candidates, new_cursor = plugin.check_features(source, database, cursor)
                if new_cursor:
                    cursor_updates[cursor_key] = new_cursor
                for candidate in candidates:
                    evaluated = evaluate_relevance(candidate, database, relevance_rules)
                    if silent_baseline:
                        if evaluated.relevant:
                            record_feature(state, evaluated)
                        else:
                            record_irrelevant(state, evaluated)
                        continue
                    updates.append(evaluated)
            except Exception as exc:
                logger.exception(
                    "Feature check failed for %s (%s)", database["id"], source_type
                )
                errors.append(
                    {
                        "database_id": database["id"],
                        "component_id": source.get("url") or source.get("repo", ""),
                        "source_type": source_type or "unknown",
                        "error": str(exc),
                        "timestamp": utc_now_iso(),
                    }
                )

        return updates, errors, cursor_updates

    def _resolve_status(
        self, errors: List[Dict[str, str]], databases: List[Dict[str, Any]]
    ) -> str:
        total_checks = sum(
            len(db.get("components", [])) + len(db.get("sources", [])) for db in databases
        )
        if not errors:
            return "SUCCESS"
        if total_checks == 0 or len(errors) >= total_checks:
            return "FAILURE"
        return "PARTIAL_SUCCESS"

    def _version_to_dict(self, update: VersionUpdate) -> Dict[str, Any]:
        return {
            "database_id": update.database_id,
            "database_name": update.database_name,
            "component_id": update.component_id,
            "component_type": update.component_type.value,
            "source_type": update.source_type,
            "current_version": update.current_version,
            "latest_version": update.latest_version,
            "new_versions": update.new_versions,
            "update_level": update.update_level.value,
            "url": update.url,
            "adapter_package": update.adapter_package,
            "dedup_key": update.dedup_key(),
        }

    def _feature_to_dict(self, update: FeatureUpdate) -> Dict[str, Any]:
        return {
            "database_id": update.database_id,
            "database_name": update.database_name,
            "title": update.title,
            "description": update.description,
            "source_type": update.source_type,
            "matched_keywords": update.matched_keywords,
            "relevant": update.relevant,
            "relevance_reason": update.relevance_reason,
            "url": update.url,
            "adapter_capabilities": update.adapter_capabilities,
            "version": update.version,
            "dedup_key": update.dedup_key(),
        }

    @staticmethod
    def _normalize_tag(value: str) -> str:
        cleaned = value.strip().lower()
        if cleaned.startswith("v") and len(cleaned) > 1 and cleaned[1].isdigit():
            cleaned = cleaned[1:]
        return cleaned.split()[0].strip("()")

    def _feature_tag(self, feature: FeatureUpdate) -> Optional[str]:
        raw = feature.version
        if not raw and feature.url and "/releases/tag/" in feature.url:
            raw = feature.url.rsplit("/releases/tag/", 1)[-1]
        if not raw:
            return None
        return self._normalize_tag(raw)

    def _drop_version_only_features(
        self,
        versions: List[VersionUpdate],
        features: List[FeatureUpdate],
        databases: List[Dict[str, Any]],
    ) -> List[FeatureUpdate]:
        version_tags = set()
        for update in versions:
            version_tags.add((update.database_id, self._normalize_tag(update.latest_version)))
            for item in update.new_versions:
                version_tags.add((update.database_id, self._normalize_tag(item)))

        watch_by_db = {
            database["id"]: {item.lower() for item in database.get("watch_features", [])}
            for database in databases
        }
        names_by_db = {
            database["id"]: {database["id"].lower(), database.get("name", "").lower()}
            for database in databases
        }
        kept: List[FeatureUpdate] = []
        for feature in features:
            if feature.source_type != "github_releases":
                kept.append(feature)
                continue
            tag = self._feature_tag(feature)
            watches = watch_by_db.get(feature.database_id, set())
            generic = names_by_db.get(feature.database_id, {feature.database_id.lower()})
            matched = {item.lower() for item in feature.matched_keywords} - generic
            has_feature_signal = bool(matched & watches) or "adapter" in feature.relevance_reason
            if tag and (feature.database_id, tag) in version_tags and not has_feature_signal:
                continue
            kept.append(feature)
        return kept
