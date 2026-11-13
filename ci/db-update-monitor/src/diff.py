"""Diff engine: first-run baseline and deduplication."""

from __future__ import annotations

from typing import List, Tuple

from models import FeatureUpdate, MonitorState, VersionUpdate
from state import record_component_check, record_feature, record_irrelevant
from version_normalizer import VersionNormalizer

NORMALIZER = VersionNormalizer()


def partition_updates(
    state: MonitorState,
    updates: List[VersionUpdate],
) -> Tuple[List[VersionUpdate], List[VersionUpdate]]:
    """Return (to_notify, already_seen_or_baselined)."""
    to_notify: List[VersionUpdate] = []
    skipped: List[VersionUpdate] = []

    for update in updates:
        key = update.component_key()
        previous_pin = state.pin_snapshot.get(key)
        current_pin = update.current_version

        if previous_pin is not None and previous_pin != current_pin:
            state.pin_snapshot[key] = current_pin
            if not NORMALIZER.is_newer(update.latest_version, current_pin):
                record_component_check(state, update)
                skipped.append(update)
                continue

        state.pin_snapshot[key] = current_pin
        component_state = state.components.get(key)

        if not state.baseline_established:
            record_component_check(state, update)
            skipped.append(update)
            continue

        if component_state and component_state.last_notified_version == update.latest_version:
            skipped.append(update)
            continue

        to_notify.append(update)

    return to_notify, skipped


def partition_feature_updates(
    state: MonitorState,
    updates: List[FeatureUpdate],
) -> Tuple[List[FeatureUpdate], List[FeatureUpdate]]:
    """Return (to_notify, skipped). Only relevant features are eligible to notify."""
    to_notify: List[FeatureUpdate] = []
    skipped: List[FeatureUpdate] = []

    for update in updates:
        dedup_key = update.dedup_key()

        if dedup_key in state.seen_irrelevant:
            skipped.append(update)
            continue

        if not state.baseline_established:
            if update.relevant:
                record_feature(state, update)
            else:
                record_irrelevant(state, update)
            skipped.append(update)
            continue

        if dedup_key in state.features:
            skipped.append(update)
            continue

        if not update.relevant:
            record_irrelevant(state, update)
            skipped.append(update)
            continue

        to_notify.append(update)

    return to_notify, skipped


def apply_notifications(
    state: MonitorState,
    version_updates: List[VersionUpdate],
    feature_updates: List[FeatureUpdate] | None = None,
) -> None:
    for update in version_updates:
        record_component_check(state, update)
    for update in feature_updates or []:
        record_feature(state, update)


def establish_baseline(state: MonitorState) -> None:
    if not state.baseline_established:
        state.baseline_established = True
