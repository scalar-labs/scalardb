"""Maven Central version drift checker."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

import requests

from models import ComponentType, VersionUpdate
from plugins.base import SourcePlugin
from version_normalizer import VersionNormalizer


class MavenVersionChecker(SourcePlugin):
    def __init__(self) -> None:
        self.normalizer = VersionNormalizer()

    def plugin_type(self) -> str:
        return "maven"

    def check(
        self,
        upstream_config: Dict[str, Any],
        database: Dict[str, Any],
        component: Dict[str, Any],
        current_pin: str,
    ) -> Optional[VersionUpdate]:
        artifact = upstream_config["artifact"]
        group_id, artifact_id = artifact.split(":", 1)
        versions = self._fetch_versions(group_id, artifact_id, upstream_config)
        granularity = component.get("version_granularity", upstream_config.get("version_granularity", "exact"))
        newer = self.normalizer.filter_by_granularity(versions, current_pin, granularity)
        if not newer:
            return None

        latest = self.normalizer.max_version(newer) or newer[0]
        return VersionUpdate(
            database_id=database["id"],
            database_name=database["name"],
            component_id=component["id"],
            component_type=ComponentType(component.get("type", "sdk")),
            source_type=self.plugin_type(),
            current_version=current_pin,
            latest_version=latest,
            new_versions=newer,
            update_level=self.normalizer.update_level(current_pin, latest),
            url=f"https://search.maven.org/artifact/{group_id}/{artifact_id}/{latest}",
            adapter_package=database.get("adapter_package"),
        )

    def _fetch_versions(
        self, group_id: str, artifact_id: str, upstream_config: Dict[str, Any]
    ) -> List[str]:
        repository = upstream_config.get("repository", "https://repo1.maven.org/maven2")
        group_path = group_id.replace(".", "/")
        metadata_url = f"{repository.rstrip('/')}/{group_path}/{artifact_id}/maven-metadata.xml"
        timeout = upstream_config.get("timeout_seconds", 30)

        response = requests.get(metadata_url, timeout=(10, timeout))
        response.raise_for_status()
        root = ET.fromstring(response.content)
        versions = [element.text for element in root.findall(".//version") if element.text]
        if not versions:
            latest = root.findtext("version")
            if latest:
                versions = [latest]
        return versions
