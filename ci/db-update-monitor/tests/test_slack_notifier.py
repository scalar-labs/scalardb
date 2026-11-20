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


def test_build_slack_payload_splits_long_feature_section():
    features = [
        FeatureUpdate(
            database_id="s3",
            database_name="Amazon S3",
            title=f"Announcement {index} " + ("x" * 80),
            description="y" * 200,
            source_type="rss",
            matched_keywords=["Amazon S3"],
            url=f"https://example.com/{index}",
        )
        for index in range(20)
    ]
    payload = build_slack_payload([], features, errors=[])
    section_texts = [block["text"]["text"] for block in payload["blocks"] if block["type"] == "section"]
    assert any("FEATURE updates (cont.)" in text for text in section_texts)
    assert all(len(text) <= 3000 for text in section_texts)


def _feature(index: int, **overrides) -> FeatureUpdate:
    data = dict(
        database_id="cosmos",
        database_name="Azure Cosmos DB",
        title=f"Announcement {index}",
        description="d" * 500,
        source_type="rss",
        matched_keywords=[],
        relevant=True,
        url=f"https://example.com/{index}",
    )
    data.update(overrides)
    return FeatureUpdate(**data)


def _sections(payload):
    return [block["text"]["text"] for block in payload["blocks"] if block["type"] == "section"]


def test_build_slack_payload_escapes_control_characters():
    payload = build_slack_payload([], [_feature(1, title="Q&A <live>")], errors=[])
    text = _sections(payload)[1]
    assert "Q&amp;A &lt;live&gt;" in text
    assert "<https://example.com/1|View announcement>" in text


def test_build_slack_payload_long_errors_stay_under_section_limit():
    errors = [
        {"database_id": "x", "component_id": "y", "source_type": "rss", "error": "E" * 900}
        for _ in range(5)
    ]
    payload = build_slack_payload([], [], errors=errors)
    assert all(len(text) <= 3000 for text in _sections(payload))


def test_build_slack_payload_caps_blocks_and_keeps_errors():
    errors = [{"database_id": "x", "component_id": "y", "source_type": "rss", "error": "boom"}]
    payload = build_slack_payload(
        [], [_feature(index) for index in range(400)], errors=errors, run_url="https://run"
    )
    sections = _sections(payload)
    assert len(payload["blocks"]) <= 50
    assert any("Message shortened" in text for text in sections)
    assert any("Check failures" in text for text in sections)
    assert payload["blocks"][-1]["type"] == "context"


def test_build_slack_payload_has_fallback_text():
    errors = [{"database_id": "x", "component_id": "y", "source_type": "rss", "error": "boom"}]
    payload = build_slack_payload([], [_feature(1), _feature(2)], errors=errors)
    assert payload["text"] == "ScalarDB Database Update Monitor: 2 new update(s), 1 check failure(s)"


def test_split_never_cuts_an_item_in_half():
    payload = build_slack_payload([], [_feature(index) for index in range(20)], errors=[])
    sections = _sections(payload)[1:]
    assert len(sections) > 1
    for text in sections:
        body = text.split("\n", 1)[1]
        assert body.startswith("• "), body[:40]
        assert body.rstrip().endswith("|View announcement>")
