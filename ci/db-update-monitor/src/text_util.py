"""Small text helpers shared by feature plugins."""

from __future__ import annotations

import re
from datetime import datetime
from typing import List, Optional

_WHITESPACE = re.compile(r"\s+")
_SHORT_TOKEN = 4
USER_AGENT = "ScalarDB-Database-Update-Monitor/1.0"


def keyword_in_text(text: str, keyword: str) -> bool:
    """Match phrases by substring; short tokens by word boundary."""
    needle = keyword.lower().strip()
    haystack = text.lower()
    if not needle:
        return False
    if " " in needle or len(needle) > _SHORT_TOKEN:
        return needle in haystack
    return re.search(rf"(?<![a-z0-9]){re.escape(needle)}(?![a-z0-9])", haystack) is not None


def match_keywords(text: str, keywords: List[str]) -> List[str]:
    return [keyword for keyword in keywords if keyword_in_text(text, keyword)]


def collapse_whitespace(text: str) -> str:
    return _WHITESPACE.sub(" ", text).strip()


def truncate(text: str, limit: int = 500) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def to_iso_z(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def feature_cursor_key(database_id: str, source: dict) -> str:
    source_type = source.get("type") or "unknown"
    specific = source.get("id") or source.get("url") or source.get("repo") or ""
    if specific:
        return f"{database_id}:{source_type}:{specific}"
    return f"{database_id}:{source_type}"


def resolve_cursor(cursors: dict, key: str, database_id: str, source_type: str) -> str | None:
    if key in cursors:
        return cursors[key]
    return cursors.get(f"{database_id}:{source_type}")
