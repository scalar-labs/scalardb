"""GitHub Releases version checker."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from models import ComponentType, VersionUpdate
from plugins.base import SourcePlugin
from plugins.github_client import fetch_releases
from version_normalizer import VersionNormalizer


class GitHubReleaseChecker(SourcePlugin):
    def __init__(self) -> None:
        self.normalizer = VersionNormalizer()

    def plugin_type(self) -> str:
        return "github_releases"

    def check(
        self,
        upstream_config: Dict[str, Any],
        database: Dict[str, Any],
        component: Dict[str, Any],
        current_pin: str,
    ) -> Optional[VersionUpdate]:
        repo = upstream_config["repo"]
        releases = fetch_releases(
            repo,
            timeout_seconds=upstream_config.get("timeout_seconds", 30),
            per_page=30,
            include_prerelease=upstream_config.get("include_prerelease", False),
        )
        tag_strip_prefix = upstream_config.get("tag_strip_prefix", "")

        versions: List[str] = []
        release_url: Optional[str] = None
        for release in releases:
            raw_tag = release.get("tag_name", "")
            tag = raw_tag.removeprefix(tag_strip_prefix).lstrip("vV")
            if self.normalizer.is_newer(tag, current_pin):
                versions.append(tag)
                if release_url is None:
                    release_url = release.get("html_url")

        granularity = component.get(
            "version_granularity", upstream_config.get("version_granularity", "exact")
        )
        newer = self.normalizer.filter_by_granularity(versions, current_pin, granularity)
        if not newer:
            return None

        latest = self.normalizer.max_version(newer) or newer[0]
        return VersionUpdate(
            database_id=database["id"],
            database_name=database["name"],
            component_id=component["id"],
            component_type=ComponentType(component.get("type", "server")),
            source_type=self.plugin_type(),
            current_version=current_pin,
            latest_version=latest,
            new_versions=newer,
            update_level=self.normalizer.update_level(current_pin, latest),
            url=release_url,
            adapter_package=database.get("adapter_package"),
        )
