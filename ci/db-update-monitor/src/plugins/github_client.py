"""Shared GitHub Releases API client."""

from __future__ import annotations

import os
from typing import Any, Dict, List

import requests

from text_util import USER_AGENT

API_URL = "https://api.github.com"


def fetch_releases(
    repo: str,
    timeout_seconds: int = 30,
    per_page: int = 30,
    include_prerelease: bool = False,
) -> List[Dict[str, Any]]:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": USER_AGENT,
    }
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    response = requests.get(
        f"{API_URL}/repos/{repo}/releases",
        headers=headers,
        params={"per_page": per_page},
        timeout=(10, timeout_seconds),
    )
    response.raise_for_status()
    releases = response.json()
    if not include_prerelease:
        releases = [release for release in releases if not release.get("prerelease")]
    return releases
