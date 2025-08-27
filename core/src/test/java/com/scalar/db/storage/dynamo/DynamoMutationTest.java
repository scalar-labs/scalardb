package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class DynamoMutationTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
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

  @Mock private TableMetadata metadata;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    when(metadata.getPartitionKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_1)));
  }

  private Put preparePut() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_2);
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .intValue(ANY_NAME_3, ANY_INT_1)
        .intValue(ANY_NAME_4, ANY_INT_2)
        .build();
  }

  @Test
  public void getIfNotExistsCondition_PutGiven_ShouldReturnCondition() {
    // Arrange
    Put put = preparePut();
    DynamoMutation dynamoMutation = new DynamoMutation(put, metadata);

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
    DynamoMutation dynamoMutation = new DynamoMutation(put, metadata);

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
        ConditionBuilder.putIf(
                ConditionBuilder.column(ANY_NAME_3).isEqualToInt(ANY_INT_VALUE.get()))
            .and(ConditionBuilder.column(ANY_NAME_4).isGreaterThanInt(ANY_INT_VALUE.get()))
            .build();
    Put put = Put.newBuilder(preparePut()).condition(conditions).build();

    DynamoMutation dynamoMutation = new DynamoMutation(put, metadata);

    // Act
    String actual = dynamoMutation.getCondition();

    // Assert
    assertThat(actual)
        .isEqualTo(
            DynamoOperation.CONDITION_COLUMN_NAME_ALIAS
                + "0 = "
                + DynamoOperation.CONDITION_VALUE_ALIAS
                + "0 AND "
                + DynamoOperation.CONDITION_COLUMN_NAME_ALIAS
                + "1 > "
                + DynamoOperation.CONDITION_VALUE_ALIAS
                + "1");
  }

  @Test
  public void getConditionColumnMap_PutGiven_ShouldReturnCondition() {
    // Arrange
    PutIf conditions =
        ConditionBuilder.putIf(
                ConditionBuilder.column(ANY_NAME_3).isEqualToInt(ANY_INT_VALUE.get()))
            .and(ConditionBuilder.column(ANY_NAME_4).isGreaterThanInt(ANY_INT_VALUE.get()))
            .build();
    Put put = Put.newBuilder(preparePut()).condition(conditions).build();

    Map<String, String> expected = new HashMap<>();
    expected.put(DynamoOperation.CONDITION_COLUMN_NAME_ALIAS + "0", ANY_NAME_3);
    expected.put(DynamoOperation.CONDITION_COLUMN_NAME_ALIAS + "1", ANY_NAME_4);

    DynamoMutation dynamoMutation = new DynamoMutation(put, metadata);

    // Act
    Map<String, String> conditionColumnMap = dynamoMutation.getConditionColumnMap();

    // Assert
    assertThat(conditionColumnMap).isEqualTo(expected);
  }

  @Test
  public void getUpdateExpression_PutWithIfExistsGiven_ShouldReturnExpression() {
    // Arrange
    Put put = Put.newBuilder(preparePut()).condition(ConditionBuilder.putIfExists()).build();
    DynamoMutation dynamoMutation = new DynamoMutation(put, metadata);

    // Act
    String actual = dynamoMutation.getUpdateExpression();

    // Assert
    assertThat(actual)
        .isEqualTo(
            "SET "
                + DynamoOperation.COLUMN_NAME_ALIAS
                + "0 = "
                + DynamoOperation.VALUE_ALIAS
                + "0, "
                + DynamoOperation.COLUMN_NAME_ALIAS
                + "1 = "
                + DynamoOperation.VALUE_ALIAS
                + "1");
  }

  @Test
  public void getConditionBindMap_PutWithPutIfGiven_ShouldReturnBindMap() {
    // Arrange
    PutIf conditions =
        ConditionBuilder.putIf(
                ConditionBuilder.column(ANY_NAME_3).isEqualToInt(ANY_INT_VALUE.get()))
            .and(ConditionBuilder.column(ANY_NAME_4).isGreaterThanInt(ANY_INT_VALUE.get()))
            .build();
    Put put = Put.newBuilder(preparePut()).condition(conditions).build();
    Map<String, AttributeValue> expected = new HashMap<>();
    expected.put(
        DynamoOperation.CONDITION_VALUE_ALIAS + "0",
        AttributeValue.builder().n(String.valueOf(ANY_INT_3)).build());
    expected.put(
        DynamoOperation.CONDITION_VALUE_ALIAS + "1",
        AttributeValue.builder().n(String.valueOf(ANY_INT_3)).build());

    DynamoMutation dynamoMutation = new DynamoMutation(put, metadata);

    // Act
    Map<String, AttributeValue> actual = dynamoMutation.getConditionBindMap();

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void getValueBindMap_PutWithPutIfExistsGiven_ShouldReturnBindMap() {
    // Arrange
    Put put = Put.newBuilder(preparePut()).condition(ConditionBuilder.putIfExists()).build();
    Map<String, AttributeValue> expected = new HashMap<>();
    expected.put(
        DynamoOperation.VALUE_ALIAS + "0",
        AttributeValue.builder().n(String.valueOf(ANY_INT_1)).build());
    expected.put(
        DynamoOperation.VALUE_ALIAS + "1",
        AttributeValue.builder().n(String.valueOf(ANY_INT_2)).build());

    DynamoMutation dynamoMutation = new DynamoMutation(put, metadata);

    // Act
    Map<String, AttributeValue> actual = dynamoMutation.getValueBindMap();

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void getValueBindMap_PutWithNullValueGiven_ShouldReturnBindMap() {
    // Arrange
    Put put = Put.newBuilder(preparePut()).intValue(ANY_NAME_3, null).build();
    Map<String, AttributeValue> expected = new HashMap<>();
    expected.put(DynamoOperation.VALUE_ALIAS + "0", AttributeValue.builder().nul(true).build());
    expected.put(
        DynamoOperation.VALUE_ALIAS + "1",
        AttributeValue.builder().n(String.valueOf(ANY_INT_2)).build());

    DynamoMutation dynamoMutation = new DynamoMutation(put, metadata);

    // Act
    Map<String, AttributeValue> actual = dynamoMutation.getValueBindMap();

    // Assert
    assertThat(actual).isEqualTo(expected);
  }
}
