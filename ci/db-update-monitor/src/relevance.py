"""Rules-based relevance gate for feature updates."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from config_loader import load_yaml
from models import FeatureUpdate
from text_util import keyword_in_text, match_keywords

_LEGACY_ADAPTER_HINTS = (
    "v1",
    "single_path",
    "single-path",
    "legacy",
)


def load_relevance_rules(path: Path) -> Dict[str, List[str]]:
    if not path.exists():
        return {"irrelevant_keywords": [], "relevant_keywords": []}
    raw = load_yaml(path)
    return {
        "irrelevant_keywords": [k.lower() for k in raw.get("irrelevant_keywords", [])],
        "relevant_keywords": [k.lower() for k in raw.get("relevant_keywords", [])],
    }


def _text_blob(feature: FeatureUpdate) -> str:
    return f"{feature.title} {feature.description}"


def _adapter_gap_reason(text: str, database: Dict[str, Any]) -> Optional[str]:
    capabilities = database.get("adapter_capabilities") or {}
    watch_features = database.get("watch_features") or []
    if not capabilities:
        return None

    for key, value in capabilities.items():
        value_text = str(value).lower()
        legacy = any(hint in value_text for hint in _LEGACY_ADAPTER_HINTS)
        if not legacy:
            continue
        matched = match_keywords(text, watch_features)
        if matched:
            return f"adapter {key}={value} vs {matched[0]}"
        if keyword_in_text(text, "hierarchical") or keyword_in_text(text, "v2"):
            return f"adapter {key}={value} vs newer capability in announcement"
    return None


def evaluate_relevance(
    feature: FeatureUpdate,
    database: Dict[str, Any],
    rules: Dict[str, List[str]],
) -> FeatureUpdate:
    """Decide Slack relevance. include_all only means the feed was ingested."""
    text = _text_blob(feature)
    watch_features = list(database.get("watch_features", []))

    for irrelevant in rules.get("irrelevant_keywords", []):
        if keyword_in_text(text, irrelevant):
            feature.relevant = False
            feature.relevance_reason = f"matched irrelevant keyword: {irrelevant}"
            return feature

    gap = _adapter_gap_reason(text, database)
    if gap:
        feature.relevant = True
        feature.relevance_reason = gap
        return feature

    watch_hits = match_keywords(text, watch_features)
    if watch_hits:
        feature.matched_keywords = list(dict.fromkeys(feature.matched_keywords + watch_hits))
        feature.relevant = True
        feature.relevance_reason = f"matches watch_features: {watch_hits[0]}"
        return feature

    for keyword in feature.matched_keywords:
        if keyword.lower() in [w.lower() for w in watch_features]:
            feature.relevant = True
            feature.relevance_reason = f"matches watch_features: {keyword}"
            return feature

    relevant_hits = match_keywords(text, rules.get("relevant_keywords", []))
    if relevant_hits:
        feature.relevant = True
        feature.relevance_reason = f"matches relevant keyword: {relevant_hits[0]}"
        return feature

    feature.relevant = False
    feature.relevance_reason = "no matching keywords"
    return feature
