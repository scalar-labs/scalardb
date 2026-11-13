import json

import responses

from plugins.docker import DockerHubChecker

PAGE_1 = {
    "results": [{"name": "18.1"}, {"name": "latest"}],
    "next": "https://hub.docker.com/v2/repositories/library/postgres/tags?page=2",
}
PAGE_2 = {"results": [{"name": "17.6"}, {"name": "18.0"}], "next": None}


@responses.activate
def test_docker_checker_follows_tag_pages():
    def paging(request):
        payload = PAGE_2 if "page=2" in request.url else PAGE_1
        return (200, {"Content-Type": "application/json"}, json.dumps(payload))

    responses.add_callback(
        responses.GET,
        "https://hub.docker.com/v2/repositories/library/postgres/tags",
        callback=paging,
    )
    checker = DockerHubChecker()
    result = checker.check(
        {
            "image": "postgres",
            "tag_pattern": r"^\d+(\.\d+)?$",
            "exclude_tags": ["latest"],
            "version_granularity": "major",
        },
        {"id": "postgresql", "name": "PostgreSQL"},
        {"id": "server", "type": "server", "version_granularity": "major"},
        "17",
    )
    assert result is not None
    assert result.latest_version == "18.1"
    assert "18.0" in result.new_versions
