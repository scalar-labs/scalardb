"""Version parsing, comparison, and granularity filtering."""

from __future__ import annotations

import re
from functools import cmp_to_key
from typing import List, Optional, Tuple, Union

from packaging.version import InvalidVersion, Version

from models import UpdateLevel

NON_DIGIT_PREFIX = re.compile(r"^[^0-9]*")
PRE_RELEASE_TOKEN = re.compile(
    r"(beta|alpha|rc|preview|snapshot|milestone|(?:^|[.\-])m\d+)",
    re.IGNORECASE,
)
# Splits vendor schemes that packaging cannot parse, e.g. "13.6.0.jre8" or "42.7.3-yb-4".
VERSION_TOKEN = re.compile(r"\d+|[a-z]+")


def _compare_tokens(left: List[Union[int, str]], right: List[Union[int, str]]) -> int:
    for left_token, right_token in zip(left, right):
        if left_token == right_token:
            continue
        if type(left_token) is type(right_token):
            return -1 if left_token < right_token else 1
        # A number is newer than a word, e.g. "1.0.1" > "1.0.final".
        return 1 if isinstance(left_token, int) else -1
    return (len(left) > len(right)) - (len(left) < len(right))


class VersionNormalizer:
    def normalize(self, version: str) -> str:
        cleaned = version.strip()
        cleaned = NON_DIGIT_PREFIX.sub("", cleaned)
        return cleaned.lstrip("vV")

    def parse(self, version: str) -> Optional[Version]:
        normalized = self.normalize(version)
        try:
            return Version(normalized)
        except InvalidVersion:
            return None

    def tokens(self, version: str) -> List[Union[int, str]]:
        return [
            int(token) if token.isdigit() else token
            for token in VERSION_TOKEN.findall(self.normalize(version).lower())
        ]

    def release(self, version: str) -> Tuple[int, ...]:
        """Leading numeric parts, e.g. (13, 6, 0) for "13.6.0.jre8"."""
        parsed = self.parse(version)
        if parsed is not None:
            return parsed.release
        parts: List[int] = []
        for token in self.tokens(version):
            if not isinstance(token, int):
                break
            parts.append(token)
        return tuple(parts)

    def compare(self, left: str, right: str) -> int:
        left_parsed = self.parse(left)
        right_parsed = self.parse(right)
        if left_parsed and right_parsed:
            if left_parsed < right_parsed:
                return -1
            if left_parsed > right_parsed:
                return 1
            return 0
        return _compare_tokens(self.tokens(left), self.tokens(right))

    def is_prerelease(self, version: str) -> bool:
        parsed = self.parse(version)
        if parsed is not None and parsed.is_prerelease:
            return True
        return PRE_RELEASE_TOKEN.search(version) is not None

    def exclude_prereleases(self, versions: List[str], pin: str) -> List[str]:
        if self.is_prerelease(pin):
            return versions
        return [version for version in versions if not self.is_prerelease(version)]

    def is_newer(self, candidate: str, baseline: str) -> bool:
        return self.compare(candidate, baseline) > 0

    def update_level(self, baseline: str, latest: str) -> UpdateLevel:
        base = self.release(baseline)
        latest_release = self.release(latest)
        if not base or not latest_release:
            return UpdateLevel.UNKNOWN
        base = base + (0,) * (3 - len(base))
        latest_release = latest_release + (0,) * (3 - len(latest_release))
        if latest_release[0] > base[0]:
            return UpdateLevel.MAJOR
        if latest_release[:2] > base[:2]:
            return UpdateLevel.MINOR
        if latest_release[:3] > base[:3]:
            return UpdateLevel.PATCH
        return UpdateLevel.UNKNOWN

    def sort_desc(self, versions: List[str]) -> List[str]:
        return sorted(versions, key=cmp_to_key(self.compare), reverse=True)

    def newer_than(self, versions: List[str], baseline: str) -> List[str]:
        versions = self.exclude_prereleases(versions, baseline)
        result = [version for version in versions if self.is_newer(version, baseline)]
        return self.sort_desc(result)

    def filter_by_granularity(
        self, versions: List[str], baseline: str, granularity: str
    ) -> List[str]:
        newer = self.newer_than(versions, baseline)
        if granularity not in ("minor", "major"):
            return newer
        base = self.release(baseline)
        if not base:
            return newer
        width = 2 if granularity == "minor" else 1
        base_key = (base + (0,) * width)[:width]
        filtered: List[str] = []
        for version in newer:
            parts = self.release(version)
            if parts and (parts + (0,) * width)[:width] > base_key:
                filtered.append(version)
        return filtered

    def max_version(self, versions: List[str]) -> Optional[str]:
        if not versions:
            return None
        return self.sort_desc(versions)[0]
