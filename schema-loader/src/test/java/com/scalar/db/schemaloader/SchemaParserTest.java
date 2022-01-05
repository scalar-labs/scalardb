package com.scalar.db.schemaloader;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class SchemaParserTest {

  @Test
  public void parse_ProperSerializedSchemaJsonAndOptionsGiven_ShouldParseCorrectly()
      throws SchemaLoaderException {
    // Arrange
    String serializedSchemaJson =
        "{"
            + "  \"sample_db.sample_table\": {"
            + "    \"transaction\": false,"
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
            + "  },"
            + "  \"sample_db.sample_table1\": {"
            + "    \"transaction\": true,"
            + "    \"partition-key\": ["
            + "      \"c1\""
            + "    ],"
            + "    \"clustering-key\": ["
            + "      \"c4\""
            + "    ],"
            + "    \"columns\": {"
            + "      \"c1\": \"INT\","
            + "      \"c2\": \"TEXT\","
            + "      \"c3\": \"INT\","
            + "      \"c4\": \"INT\","
            + "      \"c5\": \"BOOLEAN\""
            + "    }"
            + "  },"
            + "  \"sample_db2.sample_table2\": {"
            + "    \"transaction\": false,"
            + "    \"partition-key\": ["
            + "      \"c1\""
            + "    ],"
            + "    \"clustering-key\": ["
            + "      \"c4\","
            + "      \"c3\""
            + "    ],"
            + "    \"columns\": {"
            + "      \"c1\": \"INT\","
            + "      \"c2\": \"TEXT\","
            + "      \"c3\": \"INT\","
            + "      \"c4\": \"INT\","
            + "      \"c5\": \"BOOLEAN\""
            + "    }"
            + "  }"
            + "}";
    Map<String, String> option = Collections.emptyMap();
    SchemaParser parser = new SchemaParser(serializedSchemaJson, option);

    // Act
    List<TableSchema> actual = parser.parse();

    // Assert
    assertThat(actual.size()).isEqualTo(3);

    assertThat(actual.get(0).getNamespace()).isEqualTo("sample_db");
    assertThat(actual.get(0).getTable()).isEqualTo("sample_table");
    assertThat(actual.get(0).isTransactionalTable()).isFalse();
    assertThat(actual.get(0).getOptions())
        .isEqualTo(ImmutableMap.of("ru", "5000", "compaction-strategy", "LCS"));
    TableMetadata tableMetadata = actual.get(0).getTableMetadata();
    assertThat(tableMetadata.getPartitionKeyNames())
        .isEqualTo(new LinkedHashSet<>(Arrays.asList("c1", "c2")));
    assertThat(tableMetadata.getClusteringKeyNames())
        .isEqualTo(new LinkedHashSet<>(Arrays.asList("c3", "c4")));
    assertThat(tableMetadata.getClusteringOrder("c3")).isEqualTo(Order.ASC);
    assertThat(tableMetadata.getClusteringOrder("c4")).isEqualTo(Order.DESC);
    assertThat(tableMetadata.getColumnNames())
        .isEqualTo(new LinkedHashSet<>(Arrays.asList("c1", "c2", "c3", "c4", "c5", "c6", "c7")));
    assertThat(tableMetadata.getColumnDataType("c1")).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType("c2")).isEqualTo(DataType.TEXT);
    assertThat(tableMetadata.getColumnDataType("c3")).isEqualTo(DataType.BLOB);
    assertThat(tableMetadata.getColumnDataType("c4")).isEqualTo(DataType.FLOAT);
    assertThat(tableMetadata.getColumnDataType("c5")).isEqualTo(DataType.BOOLEAN);
    assertThat(tableMetadata.getColumnDataType("c6")).isEqualTo(DataType.DOUBLE);
    assertThat(tableMetadata.getColumnDataType("c7")).isEqualTo(DataType.BIGINT);
    assertThat(tableMetadata.getSecondaryIndexNames())
        .isEqualTo(new HashSet<>(Arrays.asList("c5", "c6")));

    assertThat(actual.get(1).getNamespace()).isEqualTo("sample_db");
    assertThat(actual.get(1).getTable()).isEqualTo("sample_table1");
    assertThat(actual.get(1).isTransactionalTable()).isTrue();
    assertThat(actual.get(1).getOptions()).isEmpty();
    tableMetadata = actual.get(1).getTableMetadata();
    assertThat(tableMetadata.getPartitionKeyNames())
        .isEqualTo(new LinkedHashSet<>(Collections.singletonList("c1")));
    assertThat(tableMetadata.getClusteringKeyNames())
        .isEqualTo(new LinkedHashSet<>(Collections.singletonList("c4")));
    assertThat(tableMetadata.getClusteringOrder("c4")).isEqualTo(Order.ASC);
    assertThat(tableMetadata.getColumnNames())
        .isEqualTo(new LinkedHashSet<>(Arrays.asList("c1", "c2", "c3", "c4", "c5")));
    assertThat(tableMetadata.getColumnDataType("c1")).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType("c2")).isEqualTo(DataType.TEXT);
    assertThat(tableMetadata.getColumnDataType("c3")).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType("c4")).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType("c5")).isEqualTo(DataType.BOOLEAN);
    assertThat(tableMetadata.getSecondaryIndexNames()).isEmpty();

    assertThat(actual.get(2).getNamespace()).isEqualTo("sample_db2");
    assertThat(actual.get(2).getTable()).isEqualTo("sample_table2");
    assertThat(actual.get(2).isTransactionalTable()).isFalse();
    assertThat(actual.get(2).getOptions()).isEmpty();
    tableMetadata = actual.get(2).getTableMetadata();
    assertThat(tableMetadata.getPartitionKeyNames())
        .isEqualTo(new LinkedHashSet<>(Collections.singletonList("c1")));
    assertThat(tableMetadata.getClusteringKeyNames())
        .isEqualTo(new LinkedHashSet<>(Arrays.asList("c4", "c3")));
    assertThat(tableMetadata.getClusteringOrder("c4")).isEqualTo(Order.ASC);
    assertThat(tableMetadata.getClusteringOrder("c3")).isEqualTo(Order.ASC);
    assertThat(tableMetadata.getColumnNames())
        .isEqualTo(new LinkedHashSet<>(Arrays.asList("c1", "c2", "c3", "c4", "c5")));
    assertThat(tableMetadata.getColumnDataType("c1")).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType("c2")).isEqualTo(DataType.TEXT);
    assertThat(tableMetadata.getColumnDataType("c3")).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType("c4")).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType("c5")).isEqualTo(DataType.BOOLEAN);
    assertThat(tableMetadata.getSecondaryIndexNames()).isEmpty();
  }
}
