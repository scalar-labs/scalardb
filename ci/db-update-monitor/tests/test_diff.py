from diff import establish_baseline, partition_feature_updates, partition_updates
from models import ComponentState, ComponentType, FeatureUpdate, MonitorState, UpdateLevel, VersionUpdate


def _sample_version_update(latest: str = "4.85.0") -> VersionUpdate:
    return VersionUpdate(
        database_id="cosmos",
        database_name="Azure Cosmos DB",
        component_id="sdk",
        component_type=ComponentType.SDK,
        source_type="maven",
        current_version="4.82.0",
        latest_version=latest,
        new_versions=[latest],
        update_level=UpdateLevel.MINOR,
    )


def _sample_feature_update(title: str = "Hierarchical partition keys GA") -> FeatureUpdate:
    return FeatureUpdate(
        database_id="cosmos",
        database_name="Azure Cosmos DB",
        title=title,
        description="New partition key model",
        source_type="rss",
        matched_keywords=["partition key"],
        relevant=True,
        relevance_reason="matches watch_features: partition key",
        url="https://example.com/cosmos",
    )


def test_first_run_establishes_baseline_without_notify():
    state = MonitorState(baseline_established=False)
    update = _sample_version_update()
    to_notify, skipped = partition_updates(state, [update])
    establish_baseline(state)
    assert not to_notify
    assert len(skipped) == 1
    assert state.baseline_established
    assert state.components["cosmos:sdk"].last_notified_version == "4.85.0"


def test_second_run_skips_already_notified():
    state = MonitorState(baseline_established=True)
    update = _sample_version_update()
    state.components["cosmos:sdk"] = ComponentState(
        last_notified_version="4.85.0",
        last_checked="2026-01-01T00:00:00Z",
    )
    to_notify, skipped = partition_updates(state, [update])
    assert not to_notify
    assert len(skipped) == 1


def test_new_version_notifies_after_baseline():
    state = MonitorState(baseline_established=True)
    state.components["cosmos:sdk"] = ComponentState(
        last_notified_version="4.85.0",
        last_checked="2026-01-01T00:00:00Z",
    )
    update = _sample_version_update(latest="4.86.0")
    to_notify, skipped = partition_updates(state, [update])
    assert len(to_notify) == 1
    assert to_notify[0].latest_version == "4.86.0"


def test_feature_first_run_baselines_without_notify():
    state = MonitorState(baseline_established=False)
    feature = _sample_feature_update()
    to_notify, skipped = partition_feature_updates(state, [feature])
    establish_baseline(state)
    assert not to_notify
    assert len(skipped) == 1
    assert feature.dedup_key() in state.features


def test_irrelevant_feature_not_notified():
    state = MonitorState(baseline_established=True)
    feature = FeatureUpdate(
        database_id="cosmos",
        database_name="Azure Cosmos DB",
        title="Portal UI update",
        description="New pricing page",
        source_type="rss",
        matched_keywords=[],
        relevant=False,
        relevance_reason="no matching keywords",
        url="https://example.com/ui",
    )
    to_notify, skipped = partition_feature_updates(state, [feature])
    assert not to_notify
    assert feature.dedup_key() in state.seen_irrelevant


def test_relevant_feature_notifies_after_baseline():
    state = MonitorState(baseline_established=True)
    feature = _sample_feature_update()
    to_notify, skipped = partition_feature_updates(state, [feature])
    assert len(to_notify) == 1
    assert to_notify[0].title == feature.title
