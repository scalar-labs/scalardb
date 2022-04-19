package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Get;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Key;
import com.scalar.db.storage.dynamo.bytes.KeyBytesEncoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class DynamoOperationTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";

  @Mock private TableMetadata metadata;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    when(metadata.getPartitionKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_1)));
    when(metadata.getClusteringKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_2)));
  }

  private Get prepareGet() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Get(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  @Test
  public void getTableName_GetGiven_ShouldReturnTableName() {
    // Arrange
    Get get = prepareGet();
    DynamoOperation dynamoOperation = new DynamoOperation(get, metadata);

    // Act
    String actual = dynamoOperation.getTableName();

    // Assert
    assertThat(actual).isEqualTo(ANY_NAMESPACE_NAME + "." + ANY_TABLE_NAME);
  }

  @Test
  public void getKeyMap_GetGiven_ShouldReturnMap() {
    // Arrange
    Get get = prepareGet();
    DynamoOperation dynamoOperation = new DynamoOperation(get, metadata);
    Map<String, AttributeValue> expected = new HashMap<>();
    expected.put(
        DynamoOperation.PARTITION_KEY,
        AttributeValue.builder()
            .b(
                SdkBytes.fromByteBuffer(
                    new KeyBytesEncoder().encode(new Key(ANY_NAME_1, ANY_TEXT_1))))
            .build());
    expected.put(
        DynamoOperation.CLUSTERING_KEY,
        AttributeValue.builder()
            .b(
                SdkBytes.fromByteBuffer(
                    new KeyBytesEncoder().encode(new Key(ANY_NAME_2, ANY_TEXT_2))))
            .build());

    // Act
    Map<String, AttributeValue> actual = dynamoOperation.getKeyMap();

    // Assert
    assertThat(actual).isEqualTo(expected);
  }
}
