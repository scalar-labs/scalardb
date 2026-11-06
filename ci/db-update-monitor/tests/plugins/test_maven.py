import responses

from plugins.maven import MavenVersionChecker

MAVEN_METADATA = """<?xml version="1.0" encoding="UTF-8"?>
<metadata>
  <groupId>com.azure</groupId>
  <artifactId>azure-cosmos</artifactId>
  <versioning>
    <latest>4.85.0</latest>
    <release>4.85.0</release>
    <versions>
      <version>4.82.0</version>
      <version>4.85.0</version>
    </versions>
  </versioning>
</metadata>
"""


@responses.activate
def test_maven_checker_detects_newer_versions():
    responses.add(
        responses.GET,
        "https://repo1.maven.org/maven2/com/azure/azure-cosmos/maven-metadata.xml",
        body=MAVEN_METADATA,
    )

    checker = MavenVersionChecker()
    result = checker.check(
        {"artifact": "com.azure:azure-cosmos"},
        {"id": "cosmos", "name": "Azure Cosmos DB"},
        {"id": "sdk", "type": "sdk"},
        "4.82.0",
    )

    assert result is not None
    assert result.latest_version == "4.85.0"
    assert result.new_versions == ["4.85.0"]
