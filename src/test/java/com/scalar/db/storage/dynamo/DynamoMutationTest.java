package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class DynamoMutationTest {
  private static final String ANY_KEYSPACE_NAME = "keyspace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_NAME_4 = "name4";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final int ANY_INT_1 = 1;
  private static final int ANY_INT_2 = 2;
  private static final int ANY_INT_3 = 3;
  private static final IntValue ANY_INT_VALUE = new IntValue("any_int", ANY_INT_3);

  @Mock private TableMetadataManager metadataManager;
  @Mock private TableMetadata metadata;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    when(metadataManager.getTableMetadata(any(Operation.class))).thenReturn(metadata);
    when(metadata.getPartitionKeyNames())
        .thenReturn(new HashSet<String>(Arrays.asList(ANY_NAME_1)));
    when(metadata.getKeyNames()).thenReturn(Arrays.asList(ANY_NAME_1, ANY_NAME_2));
  }

  private Put preparePut() {
    Key partitionKey = new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    Key clusteringKey = new Key(new TextValue(ANY_NAME_2, ANY_TEXT_2));
    Put put =
        new Put(partitionKey, clusteringKey)
            .forNamespace(ANY_KEYSPACE_NAME)
            .forTable(ANY_TABLE_NAME)
            .withValue(new IntValue(ANY_NAME_3, ANY_INT_1))
            .withValue(new IntValue(ANY_NAME_4, ANY_INT_2));

    return put;
  }

  @Test
  public void getValueMapWithKey_PutGiven_ShouldReturnValueMap() {
    // Arrange
    Put put = preparePut();
    DynamoMutation dynamoMutation = new DynamoMutation(put, metadataManager);

    // Act
    Map<String, AttributeValue> actual = dynamoMutation.getValueMapWithKey();

    // Assert
    assertThat(actual.get(ANY_NAME_1).s()).isEqualTo(ANY_TEXT_1);
    assertThat(actual.get(ANY_NAME_2).s()).isEqualTo(ANY_TEXT_2);
    assertThat(Integer.valueOf(actual.get(ANY_NAME_3).n())).isEqualTo(ANY_INT_1);
    assertThat(Integer.valueOf(actual.get(ANY_NAME_4).n())).isEqualTo(ANY_INT_2);
  }

  @Test
  public void getIfNotExistsCondition_PutGiven_ShouldReturnCondition() {
    // Arrange
    Put put = preparePut();
    DynamoMutation dynamoMutation = new DynamoMutation(put, metadataManager);

    // Act
    String actual = dynamoMutation.getIfNotExistsCondition();

    // Assert
    assertThat(actual)
        .isEqualTo(
            "attribute_not_exists("
                + DynamoOperation.PARTITION_KEY
                + ") AND attribute_not_exists("
                + DynamoOperation.CLUSTERING_KEY
                + ")");
  }

  @Test
  public void getIfExistsCondition_PutGiven_ShouldReturnCondition() {
    // Arrange
    Put put = preparePut();
    DynamoMutation dynamoMutation = new DynamoMutation(put, metadataManager);

    // Act
    String actual = dynamoMutation.getIfExistsCondition();

    // Assert
    assertThat(actual)
        .isEqualTo(
            "attribute_exists("
                + DynamoOperation.PARTITION_KEY
                + ") AND attribute_exists("
                + DynamoOperation.CLUSTERING_KEY
                + ")");
  }

  @Test
  public void getCondition_PutGiven_ShouldReturnCondition() {
    // Arrange
    PutIf conditions =
        new PutIf(
            new ConditionalExpression(ANY_NAME_3, ANY_INT_VALUE, Operator.EQ),
            new ConditionalExpression(ANY_NAME_4, ANY_INT_VALUE, Operator.GT));
    Put put = preparePut().withCondition(conditions);

    DynamoMutation dynamoMutation = new DynamoMutation(put, metadataManager);

    // Act
    String actual = dynamoMutation.getCondition();

    // Assert
    assertThat(actual)
        .isEqualTo(
            ANY_NAME_3
                + " = "
                + DynamoOperation.CONDITION_VALUE_ALIAS
                + "0 AND "
                + ANY_NAME_4
                + " > "
                + DynamoOperation.CONDITION_VALUE_ALIAS
                + "1");
  }

  @Test
  public void getUpdateExpression_PutWithIfExistsGiven_ShouldReturnExpression() {
    // Arrange
    Put put = preparePut().withCondition(new PutIfExists());
    DynamoMutation dynamoMutation = new DynamoMutation(put, metadataManager);

    // Act
    String actual = dynamoMutation.getUpdateExpression();

    // Assert
    assertThat(actual)
        .isEqualTo(
            "SET "
                + ANY_NAME_3
                + " = "
                + DynamoOperation.VALUE_ALIAS
                + "0, "
                + ANY_NAME_4
                + " = "
                + DynamoOperation.VALUE_ALIAS
                + "1");
  }

  @Test
  public void getConditionBindMap_PutWithPutIfGiven_ShouldReturnBindMap() {
    // Arrange
    PutIf conditions =
        new PutIf(
            new ConditionalExpression(ANY_NAME_3, ANY_INT_VALUE, Operator.EQ),
            new ConditionalExpression(ANY_NAME_4, ANY_INT_VALUE, Operator.GT));
    Put put = preparePut().withCondition(conditions);
    Map<String, AttributeValue> expected = new HashMap<>();
    expected.put(
        DynamoOperation.CONDITION_VALUE_ALIAS + "0",
        AttributeValue.builder().n(String.valueOf(ANY_INT_3)).build());
    expected.put(
        DynamoOperation.CONDITION_VALUE_ALIAS + "1",
        AttributeValue.builder().n(String.valueOf(ANY_INT_3)).build());

    DynamoMutation dynamoMutation = new DynamoMutation(put, metadataManager);

    // Act
    Map<String, AttributeValue> actual = dynamoMutation.getConditionBindMap();

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void getValueBindMap_PutWithPutIfExistsGiven_ShouldReturnBindMap() {
    // Arrange
    Put put = preparePut().withCondition(new PutIfExists());
    Map<String, AttributeValue> expected = new HashMap<>();
    expected.put(
        DynamoOperation.VALUE_ALIAS + "0",
        AttributeValue.builder().n(String.valueOf(ANY_INT_1)).build());
    expected.put(
        DynamoOperation.VALUE_ALIAS + "1",
        AttributeValue.builder().n(String.valueOf(ANY_INT_2)).build());

    DynamoMutation dynamoMutation = new DynamoMutation(put, metadataManager);

    // Act
    Map<String, AttributeValue> actual = dynamoMutation.getValueBindMap();

    // Assert
    assertThat(actual).isEqualTo(expected);
  }
}
