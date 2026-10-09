"""Slack Block Kit payload builder for version and feature updates."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

from models import FeatureUpdate, UpdateLevel, VersionUpdate

LEVEL_LABEL = {
    UpdateLevel.MAJOR: "major",
    UpdateLevel.MINOR: "minor",
    UpdateLevel.PATCH: "patch",
    UpdateLevel.UNKNOWN: "update",
}

SOURCE_LABEL = {
    "maven": "Maven",
    "docker": "Docker",
    "github_releases": "GitHub",
    "rss": "RSS",
    "webpage": "Web",
}

# Slack section mrkdwn text is limited to 3000 characters.
_SECTION_LIMIT = 2800


def _component_label(update: VersionUpdate) -> str:
    mapping = {
        "sdk": "SDK",
        "jdbc-driver": "JDBC Driver",
        "server": "Server",
        "connector": "Connector",
        "driver": "Driver",
    }
    return mapping.get(update.component_type.value, update.component_id)


def _capability_context(feature: FeatureUpdate) -> Optional[str]:
    if not feature.adapter_capabilities:
        return None
    parts = [f"{key}: {value}" for key, value in feature.adapter_capabilities.items()]
    return "ScalarDB adapter — " + ", ".join(parts)


def _section(text: str) -> Dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _chunk_lines(lines: List[str], limit: int = _SECTION_LIMIT) -> List[List[str]]:
    chunks: List[List[str]] = []
    current: List[str] = []
    size = 0
    for line in lines:
        extra = len(line) + (1 if current else 0)
        if current and size + extra > limit:
            chunks.append(current)
            current = [line]
            size = len(line)
        else:
            current.append(line)
            size += extra
    if current:
        chunks.append(current)
    return chunks


def build_slack_payload(
    version_updates: List[VersionUpdate],
    feature_updates: List[FeatureUpdate],
    errors: List[Dict[str, str]],
    run_url: Optional[str] = None,
    branch_name: Optional[str] = None,
) -> Dict:
    if not version_updates and not feature_updates and not errors:
        return {"blocks": []}

    blocks: List[Dict] = []
    total = len(version_updates) + len(feature_updates)

    if version_updates or feature_updates:
        blocks.append(
            _section(f"*ScalarDB Database Update Monitor — {total} new update(s)*")
        )

    if version_updates:
        rows: List[str] = []
        for update in version_updates:
            rows.append(
                f"{update.database_name[:18]:<18} "
                f"{_component_label(update):<12} "
                f"{update.current_version:<10} "
                f"{update.latest_version:<10} "
                f"{LEVEL_LABEL[update.update_level]:<6} "
                f"{SOURCE_LABEL.get(update.source_type, update.source_type)}"
            )
            if len(update.new_versions) > 1:
                rows.append(
                    f"  └ {len(update.new_versions)} new versions: "
                    f"{', '.join(update.new_versions[:5])}"
                    f"{'...' if len(update.new_versions) > 5 else ''}"
                )
        table_header = [
            f"{'Database':<18} {'Component':<12} {'Current':<10} {'Latest':<10} {'Level':<6} Source",
            "-" * 76,
        ]
        for index, chunk in enumerate(_chunk_lines(rows)):
            title = "*VERSION updates*" if index == 0 else "*VERSION updates (cont.)*"
            body = "\n".join(table_header + chunk)
            blocks.append(_section(f"{title}\n```\n{body}\n```"))

    if feature_updates:
        feature_lines: List[str] = []
        for feature in feature_updates:
            feature_lines.append(f"• *{feature.database_name}* — {feature.title}")
            if feature.description:
                feature_lines.append(f"  {feature.description}")
            capability = _capability_context(feature)
            if capability:
                feature_lines.append(f"  _{capability}_")
            if feature.url:
                feature_lines.append(f"  <{feature.url}|View announcement>")
        for index, chunk in enumerate(_chunk_lines(feature_lines)):
            title = "*FEATURE updates*" if index == 0 else "*FEATURE updates (cont.)*"
            blocks.append(_section(title + "\n" + "\n".join(chunk)))

    if errors:
        error_lines = [":warning: *Check failures:*"]
        for error in errors[:5]:
            error_lines.append(
                f"• {error['database_id']}/{error.get('component_id', '?')} "
                f"({error['source_type']}): {error['error']}"
            )
        if len(errors) > 5:
            error_lines.append(f"• ...and {len(errors) - 5} more")
        blocks.append(_section("\n".join(error_lines)))

    context_parts = []
    if run_url:
        context_parts.append(f"<{run_url}|View run>")
    if branch_name:
        context_parts.append(f"Branch: `{branch_name}`")
    if context_parts:
        blocks.append({"type": "divider"})
        blocks.append(
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": " | ".join(context_parts)}],
            }
        )

    return {"blocks": blocks}


def write_slack_payload(path: Path, payload: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
