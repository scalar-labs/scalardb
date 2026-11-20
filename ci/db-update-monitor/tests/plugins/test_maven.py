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


MSSQL_METADATA = """<?xml version="1.0" encoding="UTF-8"?>
<metadata>
  <versioning>
    <versions>
      <version>13.6.0.jre8</version>
      <version>13.6.0.jre11</version>
      <version>13.10.0.jre8</version>
      <version>13.10.0.jre11</version>
      <version>14.1.0.jre8-preview</version>
    </versions>
  </versioning>
</metadata>
"""


@responses.activate
def test_maven_checker_applies_version_pattern():
    responses.add(
        responses.GET,
        "https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/maven-metadata.xml",
        body=MSSQL_METADATA,
    )

    result = MavenVersionChecker().check(
        {"artifact": "com.microsoft.sqlserver:mssql-jdbc", "version_pattern": r"\.jre8$"},
        {"id": "sqlserver", "name": "Microsoft SQL Server"},
        {"id": "jdbc-driver", "type": "jdbc-driver"},
        "13.6.0.jre8",
    )

    assert result is not None
    assert result.new_versions == ["13.10.0.jre8"]
    assert result.latest_version == "13.10.0.jre8"


@responses.activate
def test_maven_checker_uses_database_level_granularity():
    responses.add(
        responses.GET,
        "https://repo1.maven.org/maven2/com/azure/azure-cosmos/maven-metadata.xml",
        body=MAVEN_METADATA,
    )

    result = MavenVersionChecker().check(
        {"artifact": "com.azure:azure-cosmos"},
        {"id": "cosmos", "name": "Azure Cosmos DB", "version_granularity": "major"},
        {"id": "sdk", "type": "sdk"},
        "4.82.0",
    )

    assert result is None


DB2_METADATA = """<?xml version="1.0" encoding="UTF-8"?>
<metadata>
  <versioning>
    <versions>
      <version>12.1.4.0</version>
      <version>12.1.5.0</version>
      <version>12.1.5.0_special_89304</version>
    </versions>
  </versioning>
</metadata>
"""


@responses.activate
def test_maven_checker_ignores_db2_special_builds():
    responses.add(
        responses.GET,
        "https://repo1.maven.org/maven2/com/ibm/db2/jcc/maven-metadata.xml",
        body=DB2_METADATA,
    )

    result = MavenVersionChecker().check(
        {"artifact": "com.ibm.db2:jcc", "version_pattern": r"^\d+(\.\d+)*$"},
        {"id": "db2", "name": "IBM Db2"},
        {"id": "jdbc-driver", "type": "jdbc-driver"},
        "12.1.5.0",
    )

    assert result is None
