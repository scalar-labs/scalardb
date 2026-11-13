"""RSS/Atom feed checker for feature announcements."""

from __future__ import annotations

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional, Tuple

import feedparser
import requests

from models import FeatureUpdate
from plugins.feature_base import FeatureSourcePlugin
from text_util import USER_AGENT, collapse_whitespace, match_keywords, parse_iso_datetime, to_iso_z, truncate


def _parse_published(entry: Dict[str, Any]) -> Optional[datetime]:
    if entry.get("published_parsed"):
        return datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
    if entry.get("updated_parsed"):
        return datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
    published = entry.get("published") or entry.get("updated")
    if published:
        try:
            dt = parsedate_to_datetime(published)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (TypeError, ValueError):
            return None
    return None


def _entry_text(entry: Dict[str, Any]) -> str:
    parts = [entry.get("title", ""), entry.get("summary", ""), entry.get("description", "")]
    return " ".join(part for part in parts if part)


def _match_haystack(entry: Dict[str, Any], match_scope: str, full_text: str) -> str:
    """Shared vendor feeds need title-only matching; article bodies name unrelated products."""
    if match_scope == "title":
        return entry.get("title", "")
    return full_text


class RssFeatureChecker(FeatureSourcePlugin):
    def plugin_type(self) -> str:
        return "rss"

    def check_features(
        self,
        source_config: Dict[str, Any],
        database: Dict[str, Any],
        cursor: Optional[str],
    ) -> Tuple[List[FeatureUpdate], Optional[str]]:
        url = source_config["url"]
        timeout = source_config.get("timeout_seconds", 30)
        response = requests.get(
            url,
            timeout=(10, timeout),
            headers={"User-Agent": USER_AGENT},
        )
        response.raise_for_status()

        feed = feedparser.parse(response.content)
        keywords = source_config.get("keywords", [])
        include_all = bool(source_config.get("include_all", False)) or not keywords
        match_scope = source_config.get("match_scope", "full")

        cursor_dt = parse_iso_datetime(cursor)
        updates: List[FeatureUpdate] = []
        latest_seen: Optional[datetime] = cursor_dt

        for entry in feed.entries:
            published_dt = _parse_published(entry)
            # Advance the cursor over every entry, matched or not, so feeds that
            # rarely match keywords still baseline instead of rescanning forever.
            if published_dt and (latest_seen is None or published_dt > latest_seen):
                latest_seen = published_dt
            if cursor_dt and published_dt and published_dt <= cursor_dt:
                continue

            text = _entry_text(entry)
            matched = match_keywords(_match_haystack(entry, match_scope, text), keywords)
            if keywords and not matched:
                continue

            description = truncate(collapse_whitespace(entry.get("summary", entry.get("description", ""))))
            updates.append(
                FeatureUpdate(
                    database_id=database["id"],
                    database_name=database["name"],
                    title=collapse_whitespace(entry.get("title", "Untitled")),
                    description=description,
                    source_type=self.plugin_type(),
                    matched_keywords=matched,
                    url=entry.get("link"),
                    adapter_capabilities=database.get("adapter_capabilities"),
                    published_at=to_iso_z(published_dt) if published_dt else None,
                    include_all=include_all or bool(matched),
                )
            )

        new_cursor = to_iso_z(latest_seen) if latest_seen else cursor
        return updates, new_cursor
