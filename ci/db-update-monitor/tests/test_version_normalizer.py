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


def test_vendor_suffix_versions_compare_numerically():
    # mssql-jdbc versions cannot be parsed by packaging; "13.10" must beat "13.6".
    normalizer = VersionNormalizer()
    assert normalizer.is_newer("13.10.0.jre8", "13.6.0.jre8")
    assert not normalizer.is_newer("13.6.0.jre8", "13.10.0.jre8")


def test_yugabyte_build_suffix_compares_numerically():
    normalizer = VersionNormalizer()
    assert normalizer.is_newer("42.7.3-yb-10", "42.7.3-yb-4")
    assert normalizer.is_newer("42.7.4-yb-1", "42.7.3-yb-10")
    assert not normalizer.is_newer("42.3.5-yb-8", "42.7.3-yb-4")


def test_vendor_suffix_versions_sort_and_level():
    normalizer = VersionNormalizer()
    versions = ["13.6.2.jre8", "13.10.0.jre8", "13.8.0.jre8"]
    assert normalizer.sort_desc(versions) == ["13.10.0.jre8", "13.8.0.jre8", "13.6.2.jre8"]
    assert normalizer.update_level("13.6.0.jre8", "13.6.2.jre8") == UpdateLevel.PATCH
    assert normalizer.update_level("13.6.0.jre8", "13.10.0.jre8") == UpdateLevel.MINOR
    assert normalizer.update_level("42.7.3-yb-4", "42.7.3-yb-7") == UpdateLevel.UNKNOWN


def test_granularity_applies_to_vendor_suffix_versions():
    normalizer = VersionNormalizer()
    versions = ["13.6.2.jre8", "13.8.0.jre8", "14.0.0.jre8"]
    assert normalizer.filter_by_granularity(versions, "13.6.0.jre8", "minor") == [
        "14.0.0.jre8",
        "13.8.0.jre8",
    ]
    assert normalizer.filter_by_granularity(versions, "13.6.0.jre8", "major") == ["14.0.0.jre8"]
