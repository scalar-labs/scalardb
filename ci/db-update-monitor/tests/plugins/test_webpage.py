import responses

from plugins.webpage import WebpageFeatureChecker, html_to_text


def test_html_to_text_strips_tags():
    text = html_to_text("<html><script>x</script><p>JSON collections are new</p></html>")
    assert "JSON collections are new" in text
    assert "script" not in text.lower() or "JSON" in text


@responses.activate
def test_webpage_checker_emits_keyword_snippets():
    responses.add(
        responses.GET,
        "https://example.com/notes",
        body="<html><body>Oracle adds JSON relational duality. Also deprecated JDBC APIs.</body></html>",
        status=200,
    )
    checker = WebpageFeatureChecker()
    updates, cursor = checker.check_features(
        {"type": "webpage", "url": "https://example.com/notes", "keywords": ["JSON", "deprecated"]},
        {"id": "oracle", "name": "Oracle Database", "watch_features": ["json"]},
        cursor=None,
    )
    assert cursor
    assert updates
    assert any("JSON" in update.matched_keywords or "json" in [k.lower() for k in update.matched_keywords] for update in updates)


@responses.activate
def test_webpage_checker_skips_unchanged_hash():
    body = "<html><body>JSON duality views</body></html>"
    responses.add(responses.GET, "https://example.com/notes", body=body, status=200)
    checker = WebpageFeatureChecker()
    source = {"url": "https://example.com/notes", "keywords": ["JSON"]}
    database = {"id": "oracle", "name": "Oracle Database"}
    first, cursor = checker.check_features(source, database, cursor=None)
    second, same_cursor = checker.check_features(source, database, cursor=cursor)
    assert first
    assert second == []
    assert same_cursor == cursor
