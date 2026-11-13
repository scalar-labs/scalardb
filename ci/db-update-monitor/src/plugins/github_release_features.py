"""GitHub release notes checker for feature announcements."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from models import FeatureUpdate
from plugins.feature_base import FeatureSourcePlugin
from plugins.github_client import fetch_releases
from text_util import collapse_whitespace, match_keywords, parse_iso_datetime, to_iso_z, truncate


class GitHubReleaseFeatureChecker(FeatureSourcePlugin):
    def plugin_type(self) -> str:
        return "github_releases"

    def check_features(
        self,
        source_config: Dict[str, Any],
        database: Dict[str, Any],
        cursor: Optional[str],
    ) -> Tuple[List[FeatureUpdate], Optional[str]]:
        repo = source_config["repo"]
        releases = fetch_releases(
            repo,
            timeout_seconds=source_config.get("timeout_seconds", 30),
            per_page=source_config.get("per_page", 20),
            include_prerelease=source_config.get("include_prerelease", False),
        )
        keywords = source_config.get("keywords", [])
        include_all = bool(source_config.get("include_all", False)) or not keywords

        cursor_dt = parse_iso_datetime(cursor)
        updates: List[FeatureUpdate] = []
        latest_seen: Optional[datetime] = cursor_dt

        for release in releases:
            published_raw = release.get("published_at")
            published_dt = parse_iso_datetime(published_raw)
            # Advance the cursor over every release, matched or not, so repos that
            # rarely match keywords still baseline instead of rescanning forever.
            if published_dt and (latest_seen is None or published_dt > latest_seen):
                latest_seen = published_dt
            if cursor_dt and published_dt and published_dt <= cursor_dt:
                continue

            body = release.get("body") or ""
            title = release.get("name") or release.get("tag_name", "Release")
            text = f"{title} {body}"
            matched = match_keywords(text, keywords)
            if keywords and not matched:
                continue

            description = truncate(collapse_whitespace(body))
            updates.append(
                FeatureUpdate(
                    database_id=database["id"],
                    database_name=database["name"],
                    title=f"{title} ({release.get('tag_name', '')})".strip(),
                    description=description or title,
                    source_type=self.plugin_type(),
                    matched_keywords=matched,
                    url=release.get("html_url"),
                    adapter_capabilities=database.get("adapter_capabilities"),
                    published_at=published_raw,
                    include_all=include_all or bool(matched),
                    version=release.get("tag_name"),
                )
            )

        new_cursor = to_iso_z(latest_seen) if latest_seen else cursor
        return updates, new_cursor
