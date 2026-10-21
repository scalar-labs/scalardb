"""Version parsing, comparison, and granularity filtering."""

from __future__ import annotations

import re
from typing import List, Optional, Tuple

from packaging.version import InvalidVersion, Version

from models import UpdateLevel

NON_DIGIT_PREFIX = re.compile(r"^[^0-9]*")
PRE_RELEASE_TOKEN = re.compile(
    r"(beta|alpha|rc|preview|snapshot|milestone|(?:^|[.\-])m\d+)",
    re.IGNORECASE,
)


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

    def compare(self, left: str, right: str) -> int:
        left_parsed = self.parse(left)
        right_parsed = self.parse(right)
        if left_parsed and right_parsed:
            if left_parsed < right_parsed:
                return -1
            if left_parsed > right_parsed:
                return 1
            return 0
        if left == right:
            return 0
        return -1 if left < right else 1

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
        base = self.parse(baseline)
        latest_parsed = self.parse(latest)
        if not base or not latest_parsed:
            return UpdateLevel.UNKNOWN
        if latest_parsed.major > base.major:
            return UpdateLevel.MAJOR
        if latest_parsed.minor > base.minor:
            return UpdateLevel.MINOR
        if latest_parsed.micro > base.micro:
            return UpdateLevel.PATCH
        return UpdateLevel.UNKNOWN

    def sort_desc(self, versions: List[str]) -> List[str]:
        def sort_key(value: str) -> Tuple[int, Version | str]:
            parsed = self.parse(value)
            if parsed:
                return (0, parsed)
            return (1, value)

        return sorted(versions, key=sort_key, reverse=True)

    def newer_than(self, versions: List[str], baseline: str) -> List[str]:
        versions = self.exclude_prereleases(versions, baseline)
        result = [version for version in versions if self.is_newer(version, baseline)]
        return self.sort_desc(result)

    def filter_by_granularity(
        self, versions: List[str], baseline: str, granularity: str
    ) -> List[str]:
        newer = self.newer_than(versions, baseline)
        if granularity == "exact":
            return newer
        if granularity == "minor":
            base = self.parse(baseline)
            if not base:
                return newer
            filtered: List[str] = []
            for version in newer:
                parsed = self.parse(version)
                if parsed and (parsed.minor > base.minor or parsed.major > base.major):
                    filtered.append(version)
            return filtered
        if granularity == "major":
            base = self.parse(baseline)
            if not base:
                return newer
            filtered: List[str] = []
            for version in newer:
                parsed = self.parse(version)
                if parsed and parsed.major > base.major:
                    filtered.append(version)
            return filtered
        return newer

    def max_version(self, versions: List[str]) -> Optional[str]:
        if not versions:
            return None
        return self.sort_desc(versions)[0]
