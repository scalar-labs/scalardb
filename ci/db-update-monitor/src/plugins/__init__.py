"""Plugin registry."""

from __future__ import annotations

from typing import Dict, Type

from plugins.base import SourcePlugin
from plugins.docker import DockerHubChecker
from plugins.feature_base import FeatureSourcePlugin
from plugins.github_release_features import GitHubReleaseFeatureChecker
from plugins.github_releases import GitHubReleaseChecker
from plugins.maven import MavenVersionChecker
from plugins.rss import RssFeatureChecker
from plugins.webpage import WebpageFeatureChecker

PLUGIN_REGISTRY: Dict[str, Type[SourcePlugin]] = {
    "maven": MavenVersionChecker,
    "docker": DockerHubChecker,
    "github_releases": GitHubReleaseChecker,
}

FEATURE_PLUGIN_REGISTRY: Dict[str, Type[FeatureSourcePlugin]] = {
    "rss": RssFeatureChecker,
    "github_releases": GitHubReleaseFeatureChecker,
    "webpage": WebpageFeatureChecker,
}


def get_plugin(source_type: str) -> SourcePlugin:
    if source_type not in PLUGIN_REGISTRY:
        raise ValueError(f"Unknown source type: {source_type}")
    return PLUGIN_REGISTRY[source_type]()


def get_feature_plugin(source_type: str) -> FeatureSourcePlugin:
    if source_type not in FEATURE_PLUGIN_REGISTRY:
        raise ValueError(f"Unknown feature source type: {source_type}")
    return FEATURE_PLUGIN_REGISTRY[source_type]()
