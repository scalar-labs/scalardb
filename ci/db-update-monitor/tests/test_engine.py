import pytest

from pathlib import Path

from engine import MonitorEngine
from models import ComponentType, FeatureUpdate, UpdateLevel, VersionUpdate


def _engine() -> MonitorEngine:
    return MonitorEngine(Path("config.yml"), Path("state.json"), Path("."))


def _tidb_version() -> VersionUpdate:
    return VersionUpdate(
        database_id="tidb",
        database_name="TiDB",
        component_id="server",
        component_type=ComponentType.SERVER,
        source_type="github_releases",
        current_version="8.5.7",
        latest_version="8.5.8",
        new_versions=["8.5.8"],
        update_level=UpdateLevel.PATCH,
    )


def _tidb_feature(**overrides) -> FeatureUpdate:
    data = dict(
        database_id="tidb",
        database_name="TiDB",
        title="TiDB v8.5.8 (v8.5.8)",
        description="Bug fixes",
        source_type="github_releases",
        matched_keywords=["tidb"],
        url="https://github.com/pingcap/tidb/releases/tag/v8.5.8",
        version="v8.5.8",
    )
    data.update(overrides)
    return FeatureUpdate(**data)


def test_drop_version_only_github_release_with_product_name_title():
    engine = _engine()
    kept = engine._drop_version_only_features(
        [_tidb_version()],
        [_tidb_feature()],
        [{"id": "tidb", "name": "TiDB", "watch_features": ["tidb", "deprecated"]}],
    )
    assert kept == []


def test_keep_github_release_with_real_feature_keyword():
    engine = _engine()
    kept = engine._drop_version_only_features(
        [_tidb_version()],
        [_tidb_feature(matched_keywords=["tidb", "deprecated"])],
        [{"id": "tidb", "name": "TiDB", "watch_features": ["tidb", "deprecated"]}],
    )
    assert len(kept) == 1


def test_keep_github_release_when_tag_not_in_version_updates():
    engine = _engine()
    kept = engine._drop_version_only_features(
        [_tidb_version()],
        [_tidb_feature(url="https://github.com/pingcap/tidb/releases/tag/v9.0.0", version="v9.0.0")],
        [{"id": "tidb", "name": "TiDB", "watch_features": ["tidb"]}],
    )
    assert len(kept) == 1


def test_unknown_filter_raises(tmp_path):
    from engine import UnknownDatabaseError

    config = tmp_path / "monitors.yaml"
    config.write_text("databases:\n  - id: cosmos\n    name: Azure Cosmos DB\n", encoding="utf-8")
    engine = MonitorEngine(config, tmp_path / "state.json", tmp_path, database_filter="cosmoss")
    with pytest.raises(UnknownDatabaseError, match="cosmos"):
        engine.run(dry_run=True)


def test_status_is_failure_when_nothing_was_checked():
    assert _engine()._resolve_status([], [{"id": "empty"}]) == "FAILURE"


def test_unreadable_pin_is_reported_without_stopping_the_run(tmp_path, repo_root):
    """A renamed Gradle property must become a reported error, not a crash."""
    config = tmp_path / "monitors.yaml"
    config.write_text(
        "databases:\n"
        "  - id: exampledb\n"
        "    name: ExampleDB\n"
        "    components:\n"
        "      - id: jdbc-driver\n"
        "        current_version: {source: gradle, file: build.gradle, property: noSuchVersion}\n"
        "        upstream: {type: maven, artifact: com.example:example}\n",
        encoding="utf-8",
    )
    state_path = tmp_path / "state.json"
    report = MonitorEngine(config, state_path, repo_root).run(dry_run=False)

    assert report["last_run_status"] == "FAILURE"
    assert "noSuchVersion" in report["errors"][0]["error"]
    assert state_path.exists()
