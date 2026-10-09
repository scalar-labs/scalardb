from models import FeatureUpdate
from relevance import evaluate_relevance
from text_util import match_keywords


def test_watch_features_marks_relevant():
    feature = FeatureUpdate(
        database_id="cosmos",
        database_name="Azure Cosmos DB",
        title="New capability",
        description="Hierarchical partition keys are now GA",
        source_type="rss",
        matched_keywords=["hierarchical partition key"],
    )
    database = {"watch_features": ["hierarchical partition key", "partition key"]}
    rules = {"irrelevant_keywords": [], "relevant_keywords": []}
    result = evaluate_relevance(feature, database, rules)
    assert result.relevant
    assert "watch_features" in result.relevance_reason


def test_irrelevant_keyword_filters_out():
    feature = FeatureUpdate(
        database_id="cosmos",
        database_name="Azure Cosmos DB",
        title="Portal update",
        description="New portal ui for Cosmos DB pricing",
        source_type="rss",
        matched_keywords=["cosmos db"],
        include_all=True,
    )
    database = {"watch_features": ["partition key"]}
    rules = {
        "irrelevant_keywords": ["portal ui", "pricing"],
        "relevant_keywords": ["partition key"],
    }
    result = evaluate_relevance(feature, database, rules)
    assert not result.relevant


def test_include_all_does_not_force_notify():
    feature = FeatureUpdate(
        database_id="postgresql",
        database_name="PostgreSQL",
        title="Community meetup recap",
        description="Photos from the conference",
        source_type="rss",
        matched_keywords=[],
        include_all=True,
    )
    result = evaluate_relevance(feature, {"watch_features": ["jdbc"]}, {"irrelevant_keywords": [], "relevant_keywords": []})
    assert not result.relevant


def test_adapter_capability_gap_is_relevant():
    feature = FeatureUpdate(
        database_id="cosmos",
        database_name="Azure Cosmos DB",
        title="Hierarchical partition keys GA",
        description="Use multiple partition key paths",
        source_type="rss",
        matched_keywords=[],
    )
    database = {
        "watch_features": ["hierarchical partition key", "partition key"],
        "adapter_capabilities": {"partition_key_model": "v1_single_path"},
    }
    result = evaluate_relevance(feature, database, {"irrelevant_keywords": [], "relevant_keywords": []})
    assert result.relevant
    assert "adapter" in result.relevance_reason


def test_short_keyword_uses_word_boundary():
    assert match_keywords("Amazon API Gateway logs", ["api"]) == ["api"]
    assert match_keywords("capital mapping", ["api"]) == []
    assert match_keywords("Amazon SageMaker Feature Store", ["S3"]) == []
    assert match_keywords("Amazon S3 Object Lock", ["Amazon S3"]) == ["Amazon S3"]
