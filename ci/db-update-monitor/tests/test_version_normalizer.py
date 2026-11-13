from models import UpdateLevel
from version_normalizer import VersionNormalizer


def test_is_newer_semver():
    normalizer = VersionNormalizer()
    assert normalizer.is_newer("4.85.0", "4.82.0")
    assert not normalizer.is_newer("4.82.0", "4.85.0")


def test_update_level_major():
    normalizer = VersionNormalizer()
    assert normalizer.update_level("17", "18") == UpdateLevel.MAJOR


def test_filter_by_granularity_major():
    normalizer = VersionNormalizer()
    versions = ["18.0", "18.1", "17.5"]
    result = normalizer.filter_by_granularity(versions, "17", "major")
    assert result == ["18.1", "18.0"]


def test_excludes_prerelease_when_pin_is_stable():
    normalizer = VersionNormalizer()
    versions = ["12.36.0-beta.1", "12.35.2", "12.35.1"]
    newer = normalizer.filter_by_granularity(versions, "12.35.1", "exact")
    assert "12.36.0-beta.1" not in newer
    assert "12.35.2" in newer


def test_keeps_prerelease_when_pin_is_prerelease():
    normalizer = VersionNormalizer()
    versions = ["12.36.0-beta.2", "12.36.0-beta.1"]
    newer = normalizer.filter_by_granularity(versions, "12.36.0-beta.1", "exact")
    assert "12.36.0-beta.2" in newer
