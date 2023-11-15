package com.scalar.db.schemaloader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class ImportSchemaParserTest {

  @Test
  public void parse_ProperSerializedSchemaJsonAndOptionsGiven_ShouldParseCorrectly()
      throws SchemaLoaderException {
    // Arrange
    Map<String, String> globalOptions = ImmutableMap.of("ru", "4000", "replication-factor", "1");
    String serializedSchemaJson =
        "{"
            + "  \"sample_db.sample_table1\": {"
            + "    \"transaction\": true"
            + "  },"
            + "  \"sample_db.sample_table2\": {"
            + "    \"transaction\": false"
            + "  },"
            + "  \"sample_db.sample_table3\": {" // unrelated options are expected to be ignored
            + "    \"partition-key\": ["
            + "      \"c1\","
            + "      \"c2\""
            + "    ],"
            + "    \"clustering-key\": ["
            + "      \"c3 ASC\","
            + "      \"c4 DESC\""
            + "    ],"
            + "    \"columns\": {"
            + "      \"c1\": \"INT\","
            + "      \"c2\": \"TEXT\","
            + "      \"c3\": \"BLOB\","
            + "      \"c4\": \"FLOAT\","
            + "      \"c5\": \"BOOLEAN\","
            + "      \"c6\": \"DOUBLE\","
            + "      \"c7\": \"BIGINT\""
            + "    },"
            + "    \"secondary-index\": ["
            + "      \"c5\","
            + "      \"c6\""
            + "    ],"
            + "    \"ru\": 5000,"
            + "    \"compaction-strategy\": \"LCS\""
            + "  }"
            + "}";
    ImportSchemaParser parser = new ImportSchemaParser(serializedSchemaJson, globalOptions);

    // Act
    List<ImportTableSchema> actual = parser.parse();

    // Assert
    assertThat(actual.size()).isEqualTo(3);

    assertThat(actual.get(0).getNamespace()).isEqualTo("sample_db");
    assertThat(actual.get(0).getTable()).isEqualTo("sample_table1");
    assertThat(actual.get(0).isTransactionTable()).isTrue();
    assertThat(actual.get(0).getOptions())
        .containsOnly(entry("ru", "4000"), entry("replication-factor", "1"));

    assertThat(actual.get(1).getNamespace()).isEqualTo("sample_db");
    assertThat(actual.get(1).getTable()).isEqualTo("sample_table2");
    assertThat(actual.get(1).isTransactionTable()).isFalse();
    assertThat(actual.get(1).getOptions())
        .containsOnly(entry("ru", "4000"), entry("replication-factor", "1"));

    assertThat(actual.get(2).getNamespace()).isEqualTo("sample_db");
    assertThat(actual.get(2).getTable()).isEqualTo("sample_table3");
    assertThat(actual.get(2).isTransactionTable()).isTrue();
    assertThat(actual.get(2).getOptions())
        .containsOnly(
            entry("ru", "5000"),
            entry("compaction-strategy", "LCS"),
            entry("replication-factor", "1"));
  }
}
