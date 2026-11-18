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
    assert len(updates) == 1
    assert "JSON" in updates[0].matched_keywords
    assert "deprecated" in [k.lower() for k in updates[0].matched_keywords]


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


TOC_V1 = "<ul><li>JSON Relational Duality</li><li>Vector Search</li></ul>"
TOC_V2 = (
    "<ul><li>JSON Relational Duality</li><li>Vector Search</li>"
    "<li>Automatic Partitioning for JSON collections</li><li>Faster backups</li></ul>"
)


def _check(body, cursor):
    responses.replace(responses.GET, "https://example.com/toc", body=body, status=200)
    return WebpageFeatureChecker().check_features(
        {"type": "webpage", "url": "https://example.com/toc", "keywords": ["JSON", "partition"]},
        {"id": "oracle", "name": "Oracle Database"},
        cursor=cursor,
    )


@responses.activate
def test_webpage_checker_reports_only_new_matching_blocks():
    responses.add(responses.GET, "https://example.com/toc", body=TOC_V1, status=200)
    first, cursor = _check(TOC_V1, None)
    second, _ = _check(TOC_V2, cursor)

    assert len(second) == 1
    update = second[0]
    assert "Automatic Partitioning for JSON collections" in update.description
    assert "JSON Relational Duality" not in update.description
    assert "Faster backups" not in update.description
    assert update.dedup_key() != first[0].dedup_key()


@responses.activate
def test_webpage_checker_ignores_changes_without_keyword_matches():
    responses.add(responses.GET, "https://example.com/toc", body=TOC_V1, status=200)
    _, cursor = _check(TOC_V1, None)
    updates, new_cursor = _check(TOC_V1 + "<p>Copyright 2026</p>", cursor)

    assert updates == []
    assert new_cursor != cursor


@responses.activate
def test_webpage_checker_accepts_legacy_hash_cursor():
    responses.add(responses.GET, "https://example.com/toc", body=TOC_V1, status=200)
    updates, cursor = _check(TOC_V1, "0" * 64)

    assert len(updates) == 1
    assert ":" in cursor
