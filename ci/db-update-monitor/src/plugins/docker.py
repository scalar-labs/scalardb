"""Docker Hub image version checker."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

import requests

from models import ComponentType, VersionUpdate
from plugins.base import SourcePlugin
from text_util import USER_AGENT
from version_normalizer import VersionNormalizer

DOCKER_HUB_URL = "https://hub.docker.com/v2/repositories"


class DockerHubChecker(SourcePlugin):
    def __init__(self) -> None:
        self.normalizer = VersionNormalizer()

    def plugin_type(self) -> str:
        return "docker"

    def check(
        self,
        upstream_config: Dict[str, Any],
        database: Dict[str, Any],
        component: Dict[str, Any],
        current_pin: str,
    ) -> Optional[VersionUpdate]:
        image = upstream_config["image"]
        repository = self._repository_path(image, upstream_config.get("namespace"))
        tags = self._fetch_tags(repository, upstream_config)
        stable_tags = self._filter_tags(tags, upstream_config)
        if not stable_tags:
            return None

        granularity = component.get(
            "version_granularity", upstream_config.get("version_granularity", "major")
        )
        newer = self.normalizer.filter_by_granularity(stable_tags, current_pin, granularity)
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
            url=f"https://hub.docker.com/r/{repository}/tags",
            adapter_package=database.get("adapter_package"),
        )

    def _repository_path(self, image: str, namespace: Optional[str]) -> str:
        if "/" in image:
            return image
        if namespace:
            return f"{namespace}/{image}"
        return f"library/{image}"

    def _fetch_tags(self, repository: str, upstream_config: Dict[str, Any]) -> List[str]:
        registry = upstream_config.get("registry", "dockerhub")
        if registry != "dockerhub":
            raise ValueError(f"Unsupported docker registry: {registry}")

        tags: List[str] = []
        url: Optional[str] = f"{DOCKER_HUB_URL}/{repository}/tags"
        params: Optional[Dict[str, Any]] = {"page_size": 100, "ordering": "-last_updated"}
        timeout = upstream_config.get("timeout_seconds", 30)
        max_pages = int(upstream_config.get("max_pages", 5))
        headers = {"User-Agent": USER_AGENT}

        for _ in range(max_pages):
            if not url:
                break
            response = requests.get(url, params=params, timeout=(10, timeout), headers=headers)
            response.raise_for_status()
            payload = response.json()
            tags.extend(item["name"] for item in payload.get("results", []) if "name" in item)
            url = payload.get("next")
            params = None
        return tags

    def _filter_tags(self, tags: List[str], upstream_config: Dict[str, Any]) -> List[str]:
        tag_pattern = upstream_config.get("tag_pattern")
        exclude_tags = set(upstream_config.get("exclude_tags", []))
        exclude_pattern = upstream_config.get("exclude_pattern")
        exclude_regex = re.compile(exclude_pattern, re.IGNORECASE) if exclude_pattern else None
        pattern_regex = re.compile(tag_pattern) if tag_pattern else None

        filtered: List[str] = []
        for tag in tags:
            if tag in exclude_tags:
                continue
            if exclude_regex and exclude_regex.search(tag):
                continue
            if pattern_regex and not pattern_regex.match(tag):
                continue
            filtered.append(tag)

        return self.normalizer.sort_desc(filtered)
