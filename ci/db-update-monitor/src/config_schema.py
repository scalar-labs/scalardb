"""Validation of monitors.yaml and relevance-rules.yaml.

A misspelled or missing key would otherwise be ignored silently or fail only on the
weekly run, so the configuration is checked before anything else runs.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Set

from models import ComponentType

GRANULARITIES = {"exact", "minor", "major"}
COMPONENT_TYPES = {item.value for item in ComponentType}

DEFAULTS_KEYS = {"first_run_policy", "version_granularity"}
DATABASE_KEYS = {
    "id",
    "name",
    "enabled",
    "adapter_package",
    "watch_features",
    "adapter_capabilities",
    "adapter_gap_keywords",
    "version_granularity",
    "components",
    "sources",
}
COMPONENT_KEYS = {"id", "type", "version_granularity", "current_version", "upstream"}

# For each kind: (required keys, optional keys).
PIN_KEYS = {
    "gradle": ({"source", "file", "property"}, set()),
    "tests-config": ({"source"}, {"file", "path", "filter", "strategy"}),
}
UPSTREAM_KEYS = {
    "maven": (
        {"type", "artifact"},
        {"version_pattern", "repository", "timeout_seconds", "version_granularity"},
    ),
    "docker": (
        {"type", "image"},
        {
            "namespace",
            "registry",
            "tag_pattern",
            "exclude_tags",
            "exclude_pattern",
            "max_pages",
            "timeout_seconds",
            "version_granularity",
        },
    ),
    "github_releases": (
        {"type", "repo"},
        {
            "tag_strip_prefix",
            "version_pattern",
            "include_prerelease",
            "timeout_seconds",
            "version_granularity",
        },
    ),
}
SOURCE_KEYS = {
    "rss": ({"type", "url"}, {"id", "keywords", "include_all", "match_scope", "timeout_seconds"}),
    "github_releases": (
        {"type", "repo"},
        {"id", "keywords", "include_all", "include_prerelease", "per_page", "timeout_seconds"},
    ),
    "webpage": ({"type", "url"}, {"id", "keywords", "timeout_seconds"}),
}
REGEX_KEYS = ("version_pattern", "tag_pattern", "exclude_pattern")
RELEVANCE_RULE_KEYS = {"irrelevant_keywords", "relevant_keywords"}


class ConfigError(ValueError):
    """The configuration is invalid. The message lists every problem found."""


def _check_keys(
    errors: List[str], where: str, item: Any, required: Set[str], optional: Set[str]
) -> bool:
    if not isinstance(item, dict):
        errors.append(f"{where}: must be a mapping")
        return False
    for key in sorted(set(item) - required - optional):
        errors.append(f"{where}: unknown key '{key}'")
    for key in sorted(required - set(item)):
        errors.append(f"{where}: missing required key '{key}'")
    return True


def _check_string_list(errors: List[str], where: str, value: Any) -> None:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        errors.append(f"{where}: must be a list of strings")


def _check_granularity(errors: List[str], where: str, item: Dict[str, Any]) -> None:
    value = item.get("version_granularity")
    if value is not None and value not in GRANULARITIES:
        errors.append(
            f"{where}: version_granularity must be one of {sorted(GRANULARITIES)}, got '{value}'"
        )


def _check_regexes(errors: List[str], where: str, item: Dict[str, Any]) -> None:
    for key in REGEX_KEYS:
        if key in item:
            try:
                re.compile(item[key])
            except (re.error, TypeError) as exc:
                errors.append(f"{where}: {key} is not a valid regular expression ({exc})")


def _validate_pin(errors: List[str], where: str, pin: Any) -> None:
    source = pin.get("source") if isinstance(pin, dict) else None
    if source not in PIN_KEYS:
        errors.append(f"{where}: source must be one of {sorted(PIN_KEYS)}, got '{source}'")
        return
    required, optional = PIN_KEYS[source]
    if not _check_keys(errors, where, pin, required, optional):
        return
    if source == "tests-config":
        if ("path" in pin) == ("filter" in pin):
            errors.append(f"{where}: set exactly one of 'path' or 'filter'")
        if pin.get("strategy", "highest") != "highest":
            errors.append(f"{where}: strategy must be 'highest', got '{pin['strategy']}'")


def _validate_upstream(errors: List[str], where: str, upstream: Any) -> None:
    kind = upstream.get("type") if isinstance(upstream, dict) else None
    if kind not in UPSTREAM_KEYS:
        errors.append(f"{where}: type must be one of {sorted(UPSTREAM_KEYS)}, got '{kind}'")
        return
    required, optional = UPSTREAM_KEYS[kind]
    if not _check_keys(errors, where, upstream, required, optional):
        return
    if kind == "maven" and "artifact" in upstream and ":" not in str(upstream["artifact"]):
        errors.append(f"{where}: artifact must be 'groupId:artifactId'")
    if "exclude_tags" in upstream:
        _check_string_list(errors, f"{where}.exclude_tags", upstream["exclude_tags"])
    _check_granularity(errors, where, upstream)
    _check_regexes(errors, where, upstream)


def _validate_component(errors: List[str], where: str, component: Any) -> None:
    if not _check_keys(errors, where, component, {"id", "current_version", "upstream"}, COMPONENT_KEYS):
        return
    component_type = component.get("type")
    if component_type is not None and component_type not in COMPONENT_TYPES:
        errors.append(
            f"{where}: type must be one of {sorted(COMPONENT_TYPES)}, got '{component_type}'"
        )
    _check_granularity(errors, where, component)
    _validate_pin(errors, f"{where}.current_version", component.get("current_version"))
    _validate_upstream(errors, f"{where}.upstream", component.get("upstream"))


def _validate_source(errors: List[str], where: str, source: Any) -> None:
    kind = source.get("type") if isinstance(source, dict) else None
    if kind not in SOURCE_KEYS:
        errors.append(f"{where}: type must be one of {sorted(SOURCE_KEYS)}, got '{kind}'")
        return
    required, optional = SOURCE_KEYS[kind]
    if not _check_keys(errors, where, source, required, optional):
        return
    if "keywords" in source:
        _check_string_list(errors, f"{where}.keywords", source["keywords"])
    if source.get("match_scope", "full") not in ("full", "title"):
        errors.append(f"{where}: match_scope must be 'full' or 'title', got '{source['match_scope']}'")


def _validate_database(errors: List[str], where: str, database: Any) -> None:
    if not _check_keys(errors, where, database, {"id", "name"}, DATABASE_KEYS):
        return
    _check_granularity(errors, where, database)
    if "watch_features" in database:
        _check_string_list(errors, f"{where}.watch_features", database["watch_features"])

    capabilities = database.get("adapter_capabilities") or {}
    if not isinstance(capabilities, dict):
        errors.append(f"{where}.adapter_capabilities: must be a mapping")
        capabilities = {}
    gap_keywords = database.get("adapter_gap_keywords") or {}
    if not isinstance(gap_keywords, dict):
        errors.append(f"{where}.adapter_gap_keywords: must be a mapping")
        gap_keywords = {}
    for capability, keywords in gap_keywords.items():
        if capability not in capabilities:
            errors.append(
                f"{where}.adapter_gap_keywords: '{capability}' is not in adapter_capabilities"
            )
        _check_string_list(errors, f"{where}.adapter_gap_keywords.{capability}", keywords)

    component_ids: Set[str] = set()
    for index, component in enumerate(database.get("components") or []):
        component_id = component.get("id", index) if isinstance(component, dict) else index
        if component_id in component_ids:
            errors.append(f"{where}: duplicate component id '{component_id}'")
        component_ids.add(component_id)
        _validate_component(errors, f"{where}.components[{component_id}]", component)
    for index, source in enumerate(database.get("sources") or []):
        _validate_source(errors, f"{where}.sources[{index}]", source)


def validate_monitors_config(config: Any) -> List[str]:
    """Return a list of problems in a parsed monitors.yaml (empty when valid)."""
    errors: List[str] = []
    if not _check_keys(errors, "monitors.yaml", config, {"databases"}, {"defaults"}):
        return errors
    defaults = config.get("defaults") or {}
    if _check_keys(errors, "defaults", defaults, set(), DEFAULTS_KEYS):
        _check_granularity(errors, "defaults", defaults)

    database_ids: Set[str] = set()
    for index, database in enumerate(config.get("databases") or []):
        database_id = database.get("id", index) if isinstance(database, dict) else index
        if database_id in database_ids:
            errors.append(f"databases: duplicate id '{database_id}'")
        database_ids.add(database_id)
        _validate_database(errors, f"databases[{database_id}]", database)
    return errors


def validate_relevance_rules(rules: Any) -> List[str]:
    """Return a list of problems in a parsed relevance-rules.yaml (empty when valid)."""
    errors: List[str] = []
    if not _check_keys(errors, "relevance-rules.yaml", rules, set(), RELEVANCE_RULE_KEYS):
        return errors
    for key in RELEVANCE_RULE_KEYS & set(rules):
        _check_string_list(errors, f"relevance-rules.yaml.{key}", rules[key])
    return errors


def raise_if_invalid(errors: List[str], file_name: str) -> None:
    if errors:
        raise ConfigError(f"Invalid {file_name}:\n" + "\n".join(f"  - {error}" for error in errors))
