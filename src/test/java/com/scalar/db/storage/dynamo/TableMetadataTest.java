package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class TableMetadataTest {
  private static final String ANY_KEYSPACE_NAME = "keyspace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";

  @Test
  public void constructor_MetadataWithoutClusteringKeyGiven_ShouldConvertPropoerly() {
    // Arrange
    Map<String, AttributeValue> metadata = new HashMap<>();
    metadata.put(
        "table", AttributeValue.builder().s(ANY_KEYSPACE_NAME + "." + ANY_TABLE_NAME).build());
    metadata.put(
        "partitionKey", AttributeValue.builder().ss(Arrays.asList(ANY_NAME_1, ANY_NAME_2)).build());
    Map<String, AttributeValue> columns = new HashMap<>();
    columns.put(ANY_NAME_1, AttributeValue.builder().s("text").build());
    columns.put(ANY_NAME_2, AttributeValue.builder().s("int").build());
    columns.put(ANY_NAME_3, AttributeValue.builder().s("text").build());
    metadata.put("columns", AttributeValue.builder().m(columns).build());

    // Act
    TableMetadata actual = new TableMetadata(metadata);

    // Assert
    assertThat(actual.getPartitionKeyNames().size()).isEqualTo(2);
    assertThat(actual.getPartitionKeyNames().contains(ANY_NAME_1)).isTrue();
    assertThat(actual.getPartitionKeyNames().contains(ANY_NAME_2)).isTrue();
    assertThat(actual.getClusteringKeyNames().size()).isEqualTo(0);
    assertThat(actual.getColumns().get(ANY_NAME_1)).isEqualTo("text");
    assertThat(actual.getColumns().get(ANY_NAME_2)).isEqualTo("int");
    assertThat(actual.getColumns().get(ANY_NAME_3)).isEqualTo("text");
    assertThat(actual.getKeyNames().size()).isEqualTo(2);
    assertThat(actual.getKeyNames().contains(ANY_NAME_1)).isTrue();
    assertThat(actual.getKeyNames().contains(ANY_NAME_2)).isTrue();
  }

  @Test
  public void constructor_MetadataWithClusteringKeyGiven_ShouldConvertPropoerly() {
    // Arrange
    Map<String, AttributeValue> metadata = new HashMap<>();
    metadata.put(
        "table", AttributeValue.builder().s(ANY_KEYSPACE_NAME + "." + ANY_TABLE_NAME).build());
    metadata.put("partitionKey", AttributeValue.builder().ss(Arrays.asList(ANY_NAME_1)).build());
    metadata.put("clusteringKey", AttributeValue.builder().ss(Arrays.asList(ANY_NAME_2)).build());
    Map<String, AttributeValue> columns = new HashMap<>();
    columns.put(ANY_NAME_1, AttributeValue.builder().s("text").build());
    columns.put(ANY_NAME_2, AttributeValue.builder().s("int").build());
    columns.put(ANY_NAME_3, AttributeValue.builder().s("text").build());
    metadata.put("columns", AttributeValue.builder().m(columns).build());

    // Act
    TableMetadata actual = new TableMetadata(metadata);

    // Assert
    assertThat(actual.getPartitionKeyNames().size()).isEqualTo(1);
    assertThat(actual.getPartitionKeyNames().contains(ANY_NAME_1)).isTrue();
    assertThat(actual.getClusteringKeyNames().size()).isEqualTo(1);
    assertThat(actual.getClusteringKeyNames().contains(ANY_NAME_2)).isTrue();
    assertThat(actual.getColumns().get(ANY_NAME_1)).isEqualTo("text");
    assertThat(actual.getColumns().get(ANY_NAME_2)).isEqualTo("int");
    assertThat(actual.getColumns().get(ANY_NAME_3)).isEqualTo("text");
    assertThat(actual.getKeyNames().size()).isEqualTo(2);
    assertThat(actual.getKeyNames().contains(ANY_NAME_1)).isTrue();
    assertThat(actual.getKeyNames().contains(ANY_NAME_2)).isTrue();
  }
}
