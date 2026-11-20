"""Rules-based relevance gate for feature updates."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from config_loader import load_yaml
from config_schema import raise_if_invalid, validate_relevance_rules
from models import FeatureUpdate
from text_util import keyword_in_text, match_keywords


def load_relevance_rules(path: Path) -> Dict[str, List[str]]:
    if not path.exists():
        return {"irrelevant_keywords": [], "relevant_keywords": []}
    raw = load_yaml(path)
    raise_if_invalid(validate_relevance_rules(raw), path.name)
    return {
        "irrelevant_keywords": [k.lower() for k in raw.get("irrelevant_keywords", [])],
        "relevant_keywords": [k.lower() for k in raw.get("relevant_keywords", [])],
    }


def _text_blob(feature: FeatureUpdate) -> str:
    return f"{feature.title} {feature.description}"


def _adapter_gap_reason(text: str, database: Dict[str, Any]) -> Optional[str]:
    """Name the gap when the text mentions something the adapter does not implement yet.

    `adapter_gap_keywords` maps an `adapter_capabilities` entry to the words that
    signal a newer model, e.g. partition_key_model -> [hierarchical, v2].
    """
    capabilities = database.get("adapter_capabilities") or {}
    for capability, keywords in (database.get("adapter_gap_keywords") or {}).items():
        matched = match_keywords(text, keywords)
        if matched:
            return f"adapter {capability}={capabilities.get(capability)} vs {matched[0]}"
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
