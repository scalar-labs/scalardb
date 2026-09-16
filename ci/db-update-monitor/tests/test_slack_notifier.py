from models import ComponentType, FeatureUpdate, UpdateLevel, VersionUpdate
from notifier.slack import build_slack_payload


def test_build_slack_payload_empty():
    payload = build_slack_payload([], [], errors=[])
    assert payload["blocks"] == []


def test_build_slack_payload_version_table():
    update = VersionUpdate(
        database_id="cosmos",
        database_name="Azure Cosmos DB",
        component_id="sdk",
        component_type=ComponentType.SDK,
        source_type="maven",
        current_version="4.82.0",
        latest_version="4.85.0",
        new_versions=["4.85.0"],
        update_level=UpdateLevel.MINOR,
    )
    payload = build_slack_payload([update], [], errors=[])
    text = payload["blocks"][1]["text"]["text"]
    assert "4.85.0" in text
    assert "Azure Cosmos DB" in text
    assert "VERSION updates" in text


def test_build_slack_payload_feature_section():
    feature = FeatureUpdate(
        database_id="cosmos",
        database_name="Azure Cosmos DB",
        title="Hierarchical partition keys GA",
        description="Supports multiple partition key paths",
        source_type="rss",
        matched_keywords=["partition key"],
        relevant=True,
        adapter_capabilities={"partition_key_model": "v1_single_path"},
        url="https://example.com",
    )
    payload = build_slack_payload([], [feature], errors=[])
    text = payload["blocks"][1]["text"]["text"]
    assert "FEATURE updates" in text
    assert "Hierarchical partition keys GA" in text
    assert "v1_single_path" in text
