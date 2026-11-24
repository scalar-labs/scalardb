import responses

from plugins.github_releases import GitHubReleaseChecker


@responses.activate
def test_links_the_newest_release_not_the_first_listed():
    # The API lists by creation date: a patch on an older line can come first.
    responses.add(
        responses.GET,
        "https://api.github.com/repos/pingcap/tidb/releases",
        json=[
            {"tag_name": "v8.5.9", "html_url": "https://example.com/v8.5.9", "prerelease": False},
            {"tag_name": "v9.0.0", "html_url": "https://example.com/v9.0.0", "prerelease": False},
            {"tag_name": "v8.5.7", "html_url": "https://example.com/v8.5.7", "prerelease": False},
        ],
    )
    result = GitHubReleaseChecker().check(
        {"type": "github_releases", "repo": "pingcap/tidb"},
        {"id": "tidb", "name": "TiDB"},
        {"id": "server", "type": "server"},
        "8.5.8",
    )
    assert result.latest_version == "9.0.0"
    assert result.new_versions == ["9.0.0", "8.5.9"]
    assert result.url == "https://example.com/v9.0.0"
