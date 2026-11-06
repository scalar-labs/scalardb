"""Config-driven pin resolution from build.gradle and ci/tests-config.yaml."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List

import yaml

from version_normalizer import VersionNormalizer

GRADLE_PROPERTY_PATTERN = re.compile(r"(\w+)\s*=\s*'([^']+)'")
NORMALIZER = VersionNormalizer()


def _read_gradle_properties(repo_root: Path, file_path: str) -> Dict[str, str]:
    gradle_file = repo_root / file_path
    content = gradle_file.read_text(encoding="utf-8")
    return dict(GRADLE_PROPERTY_PATTERN.findall(content))


def _read_tests_config(repo_root: Path, file_path: str) -> Dict[str, List[str]]:
    config_file = repo_root / file_path
    data = yaml.safe_load(config_file.read_text(encoding="utf-8")) or {}
    result: Dict[str, List[str]] = {}

    for _category, entries in data.items():
        if not isinstance(entries, list):
            continue
        for entry in entries:
            versions = entry.get("versions")
            if not versions:
                continue
            label = entry["label"].replace("%VERSION%", "").rstrip("_")
            result[label] = [str(version) for version in versions]

    return result


class PinReader:
    def __init__(self, repo_root: Path):
        self.repo_root = repo_root
        self._gradle_cache: Dict[str, Dict[str, str]] = {}
        self._tests_config_cache: Dict[str, Dict[str, List[str]]] = {}

    def resolve(self, current_version_config: Dict[str, Any]) -> str:
        source = current_version_config["source"]
        if source == "gradle":
            return self._resolve_gradle(current_version_config)
        if source == "tests-config":
            return self._resolve_tests_config(current_version_config)
        raise ValueError(f"Unknown pin source: {source}")

    def _resolve_gradle(self, config: Dict[str, Any]) -> str:
        file_path = config["file"]
        property_name = config["property"]
        if file_path not in self._gradle_cache:
            self._gradle_cache[file_path] = _read_gradle_properties(self.repo_root, file_path)
        value = self._gradle_cache[file_path].get(property_name)
        if not value:
            raise ValueError(f"Gradle property not found: {property_name} in {file_path}")
        return value

    def _resolve_tests_config(self, config: Dict[str, Any]) -> str:
        file_path = config.get("file", "ci/tests-config.yaml")
        if file_path not in self._tests_config_cache:
            self._tests_config_cache[file_path] = _read_tests_config(self.repo_root, file_path)
        labels = self._tests_config_cache[file_path]

        versions: List[str] = []
        label_filter = config.get("filter")
        path = config.get("path")

        if label_filter:
            for label, label_versions in labels.items():
                if label.startswith(label_filter):
                    versions.extend(label_versions)
        elif path:
            versions = list(labels.get(path, []))
        else:
            raise ValueError("tests-config pin requires 'filter' or 'path'")

        if not versions:
            raise ValueError(f"No versions found for tests-config pin: {config}")

        strategy = config.get("strategy", "highest")
        if strategy == "all":
            return ",".join(sorted(set(versions), key=lambda v: NORMALIZER.parse(v) or v))

        return NORMALIZER.max_version(versions) or versions[-1]

    def build_pin_snapshot(self, databases: List[Dict[str, Any]]) -> Dict[str, str]:
        snapshot: Dict[str, str] = {}
        for database in databases:
            for component in database.get("components", []):
                key = f"{database['id']}:{component['id']}"
                snapshot[key] = self.resolve(component["current_version"])
        return snapshot
