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
