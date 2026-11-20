"""Slack Block Kit payload builder for version and feature updates."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

from models import FeatureUpdate, UpdateLevel, VersionUpdate
from text_util import truncate

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
# Slack rejects messages with more than 50 blocks.
_MAX_BLOCKS = 50
_ERROR_TEXT_LIMIT = 300
_MAX_ERRORS = 5


def escape(text: str) -> str:
    """Escape the characters Slack mrkdwn treats as control characters."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


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
        line = truncate(line, limit)
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


def _version_blocks(version_updates: List[VersionUpdate]) -> List[Dict]:
    # One entry per update (its row plus the "new versions" line), so a split never separates them.
    rows: List[str] = []
    for update in version_updates:
        row = (
            f"{update.database_name[:18]:<18} "
            f"{_component_label(update):<12} "
            f"{update.current_version:<10} "
            f"{update.latest_version:<10} "
            f"{LEVEL_LABEL[update.update_level]:<6} "
            f"{SOURCE_LABEL.get(update.source_type, update.source_type)}"
        )
        if len(update.new_versions) > 1:
            row += (
                f"\n  └ {len(update.new_versions)} new versions: "
                f"{', '.join(update.new_versions[:5])}"
                f"{'...' if len(update.new_versions) > 5 else ''}"
            )
        rows.append(escape(row))
    table_header = [
        f"{'Database':<18} {'Component':<12} {'Current':<10} {'Latest':<10} {'Level':<6} Source",
        "-" * 76,
    ]
    # Leave room for the title, the header rows, and the code fences.
    header_size = sum(len(line) + 1 for line in table_header) + 40
    blocks: List[Dict] = []
    for index, chunk in enumerate(_chunk_lines(rows, _SECTION_LIMIT - header_size)):
        title = "*VERSION updates*" if index == 0 else "*VERSION updates (cont.)*"
        body = "\n".join(table_header + chunk)
        blocks.append(_section(f"{title}\n```\n{body}\n```"))
    return blocks


def _feature_blocks(feature_updates: List[FeatureUpdate]) -> List[Dict]:
    # One entry per announcement, so a split never cuts an announcement in half.
    lines: List[str] = []
    for feature in feature_updates:
        entry = [f"• *{escape(feature.database_name)}* — {escape(feature.title)}"]
        if feature.description:
            entry.append(f"  {escape(feature.description)}")
        capability = _capability_context(feature)
        if capability:
            entry.append(f"  _{escape(capability)}_")
        if feature.url:
            entry.append(f"  <{feature.url}|View announcement>")
        lines.append("\n".join(entry))
    blocks: List[Dict] = []
    for index, chunk in enumerate(_chunk_lines(lines, _SECTION_LIMIT - 40)):
        title = "*FEATURE updates*" if index == 0 else "*FEATURE updates (cont.)*"
        blocks.append(_section(title + "\n" + "\n".join(chunk)))
    return blocks


def _error_blocks(errors: List[Dict[str, str]]) -> List[Dict]:
    lines = [":warning: *Check failures:*"]
    for error in errors[:_MAX_ERRORS]:
        lines.append(
            escape(
                f"• {error['database_id']}/{error.get('component_id', '?')} "
                f"({error['source_type']}): {truncate(error['error'], _ERROR_TEXT_LIMIT)}"
            )
        )
    if len(errors) > _MAX_ERRORS:
        lines.append(f"• ...and {len(errors) - _MAX_ERRORS} more")
    return [_section("\n".join(chunk)) for chunk in _chunk_lines(lines)]


def build_slack_payload(
    version_updates: List[VersionUpdate],
    feature_updates: List[FeatureUpdate],
    errors: List[Dict[str, str]],
    run_url: Optional[str] = None,
    branch_name: Optional[str] = None,
) -> Dict:
    if not version_updates and not feature_updates and not errors:
        return {"blocks": []}

    total = len(version_updates) + len(feature_updates)
    header: List[Dict] = []
    if version_updates or feature_updates:
        header.append(_section(f"*ScalarDB Database Update Monitor — {total} new update(s)*"))

    body = _version_blocks(version_updates) if version_updates else []
    if feature_updates:
        body += _feature_blocks(feature_updates)

    error_blocks = _error_blocks(errors) if errors else []

    context: List[Dict] = []
    context_parts = []
    if run_url:
        context_parts.append(f"<{run_url}|View run>")
    if branch_name:
        context_parts.append(f"Branch: `{escape(branch_name)}`")
    if context_parts:
        context.append({"type": "divider"})
        context.append(
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": " | ".join(context_parts)}],
            }
        )

    # Header, failures, and the run link always fit; shorten the update list instead.
    available = _MAX_BLOCKS - len(header) - len(error_blocks) - len(context)
    if len(body) > available:
        omitted = len(body) - (available - 1)
        body = body[: available - 1]
        body.append(
            _section(
                f"_Message shortened to fit Slack's limits: {omitted} more section(s) not shown. "
                "See report.json in the run's db-monitor-report artifact for the full list._"
            )
        )

    # Shown in notifications and previews, where blocks are not rendered.
    summary = [f"{total} new update(s)"] if total else []
    if errors:
        summary.append(f"{len(errors)} check failure(s)")
    return {
        "text": f"ScalarDB Database Update Monitor: {', '.join(summary)}",
        "blocks": header + body + error_blocks + context,
    }


def write_slack_payload(path: Path, payload: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
