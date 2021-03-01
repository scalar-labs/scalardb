package com.scalar.db.storage.dynamo;

import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class DynamoOperationTest {
  private static final String ANY_KEYSPACE_NAME = "keyspace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";

  @Mock private TableMetadataManager metadataManager;
  @Mock private DynamoTableMetadata metadata;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    when(metadataManager.getTableMetadata(any(Operation.class))).thenReturn(metadata);
    when(metadata.getPartitionKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_1)));
    when(metadata.getClusteringKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_2)));
    when(metadata.getKeyNames()).thenReturn(Arrays.asList(ANY_NAME_1, ANY_NAME_2));
  }

  private Get prepareGet() {
    Key partitionKey = new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    Key clusteringKey = new Key(new TextValue(ANY_NAME_2, ANY_TEXT_2));
    Get get =
        new Get(partitionKey, clusteringKey)
            .forNamespace(ANY_KEYSPACE_NAME)
            .forTable(ANY_TABLE_NAME);

    return get;
  }

  @Test
  public void getTableName_GetGiven_ShouldReturnTableName() {
    // Arrange
    Get get = prepareGet();
    DynamoOperation dynamoOperation = new DynamoOperation(get, metadataManager);

    // Act
    String actual = dynamoOperation.getTableName();

    // Assert
    assertThat(actual).isEqualTo(ANY_KEYSPACE_NAME + "." + ANY_TABLE_NAME);
  }

  @Test
  public void getKeyMap_GetGiven_ShouldReturnMap() {
    // Arrange
    Get get = prepareGet();
    DynamoOperation dynamoOperation = new DynamoOperation(get, metadataManager);
    Map<String, AttributeValue> expected = new HashMap<>();
    expected.put(DynamoOperation.PARTITION_KEY, AttributeValue.builder().s(ANY_TEXT_1).build());
    expected.put(DynamoOperation.CLUSTERING_KEY, AttributeValue.builder().s(ANY_TEXT_2).build());

    // Act
    Map<String, AttributeValue> actual = dynamoOperation.getKeyMap();

    // Assert
    assertThat(actual).isEqualTo(expected);
  }
}
