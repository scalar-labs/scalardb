import responses

from plugins.rss import RssFeatureChecker

SAMPLE_FEED = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Azure Updates</title>
    <item>
      <title>Cosmos DB hierarchical partition keys GA</title>
      <link>https://example.com/cosmos-v2</link>
      <pubDate>Mon, 10 Mar 2026 12:00:00 GMT</pubDate>
      <description>Hierarchical partition keys are generally available.</description>
    </item>
  </channel>
</rss>
"""


@responses.activate
def test_rss_checker_matches_keywords():
    responses.add(
        responses.GET,
        "https://example.com/feed.xml",
        body=SAMPLE_FEED,
        status=200,
    )
    checker = RssFeatureChecker()
    database = {
        "id": "cosmos",
        "name": "Azure Cosmos DB",
        "watch_features": ["partition key"],
        "adapter_capabilities": {"partition_key_model": "v1_single_path"},
    }
    source = {
        "type": "rss",
        "url": "https://example.com/feed.xml",
        "keywords": ["Cosmos DB"],
    }
    updates, cursor = checker.check_features(source, database, cursor=None)
    assert len(updates) == 1
    assert "partition" in updates[0].title.lower() or "partition" in updates[0].description.lower()
    assert updates[0].url == "https://example.com/cosmos-v2"
    assert cursor is not None


@responses.activate
def test_rss_checker_does_not_use_watch_features_as_feed_filter():
    responses.add(
        responses.GET,
        "https://example.com/feed.xml",
        body=SAMPLE_FEED,
        status=200,
    )
    checker = RssFeatureChecker()
    updates, _ = checker.check_features(
        {"type": "rss", "url": "https://example.com/feed.xml", "keywords": ["Unrelated Product"]},
        {
            "id": "cosmos",
            "name": "Azure Cosmos DB",
            "watch_features": ["partition key"],
        },
        cursor=None,
    )
    assert updates == []
