package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.Key;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

public abstract class PutStatementHandlerTestBase {
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

  private PutStatementHandler handler;
  @Mock private DynamoDbClient client;
  @Mock private TableMetadataManager metadataManager;
  @Mock private TableMetadata metadata;
  @Mock private UpdateItemResponse updateResponse;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    handler = new PutStatementHandler(client, metadataManager, getNamespacePrefix());

    when(metadataManager.getTableMetadata(any(Operation.class))).thenReturn(metadata);
    when(metadata.getPartitionKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_1)));
  }

  abstract Optional<String> getNamespacePrefix();

  private String getFullTableName() {
    return getNamespacePrefix().orElse("") + ANY_NAMESPACE_NAME + "." + ANY_TABLE_NAME;
  }

  private Put preparePut() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Put(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME)
        .withValue(ANY_NAME_3, ANY_INT_1)
        .withValue(ANY_NAME_4, ANY_INT_2);
  }

  @Test
  public void handle_PutWithoutConditionsGiven_ShouldCallUpdateItem() {
    // Arrange
    when(client.updateItem(any(UpdateItemRequest.class))).thenReturn(updateResponse);
    Put put = preparePut();
    DynamoMutation dynamoMutation = new DynamoMutation(put, metadata);
    Map<String, AttributeValue> expectedKeys = dynamoMutation.getKeyMap();
    String updateExpression = dynamoMutation.getUpdateExpressionWithKey();
    Map<String, AttributeValue> expectedBindMap = dynamoMutation.getValueBindMapWithKey();

    // Act Assert
    assertThatCode(() -> handler.handle(put)).doesNotThrowAnyException();

    // Assert
    ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
    verify(client).updateItem(captor.capture());
    UpdateItemRequest actualRequest = captor.getValue();
    assertThat(actualRequest.key()).isEqualTo(expectedKeys);
    assertThat(actualRequest.updateExpression()).isEqualTo(updateExpression);
    assertThat(actualRequest.conditionExpression()).isNull();
    assertThat(actualRequest.expressionAttributeValues()).isEqualTo(expectedBindMap);
    assertThat(actualRequest.tableName()).isEqualTo(getFullTableName());
  }

  @Test
  public void handle_PutWithoutClusteringKeyGiven_ShouldCallUpdateItem() {
    // Arrange
    when(client.updateItem(any(UpdateItemRequest.class))).thenReturn(updateResponse);
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Put put =
        new Put(partitionKey)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME)
            .withValue(ANY_NAME_3, ANY_INT_1)
            .withValue(ANY_NAME_4, ANY_INT_2);
    DynamoMutation dynamoMutation = new DynamoMutation(put, metadata);
    Map<String, AttributeValue> expectedKeys = dynamoMutation.getKeyMap();
    String updateExpression = dynamoMutation.getUpdateExpressionWithKey();
    Map<String, AttributeValue> expectedBindMap = dynamoMutation.getValueBindMapWithKey();

    // Act Assert
    assertThatCode(() -> handler.handle(put)).doesNotThrowAnyException();

    // Assert
    ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
    verify(client).updateItem(captor.capture());
    UpdateItemRequest actualRequest = captor.getValue();
    assertThat(actualRequest.key()).isEqualTo(expectedKeys);
    assertThat(actualRequest.updateExpression()).isEqualTo(updateExpression);
    assertThat(actualRequest.conditionExpression()).isNull();
    assertThat(actualRequest.expressionAttributeValues()).isEqualTo(expectedBindMap);
    assertThat(actualRequest.tableName()).isEqualTo(getFullTableName());
  }

  @Test
  public void handle_PutWithoutConditionsDynamoDbExceptionThrown_ShouldThrowExecutionException() {
    // Arrange
    Put put = preparePut();

    DynamoDbException toThrow = mock(DynamoDbException.class);
    doThrow(toThrow).when(client).updateItem(any(UpdateItemRequest.class));

    // Act Assert
    assertThatThrownBy(() -> handler.handle(put))
        .isInstanceOf(ExecutionException.class)
        .hasCause(toThrow);
  }

  @Test
  public void handle_PutIfNotExistsGiven_ShouldCallUpdateItemWithCondition() {
    // Arrange
    when(client.updateItem(any(UpdateItemRequest.class))).thenReturn(updateResponse);
    Put put = preparePut().withCondition(new PutIfNotExists());

    DynamoMutation dynamoMutation = new DynamoMutation(put, metadata);
    Map<String, AttributeValue> expectedKeys = dynamoMutation.getKeyMap();
    String updateExpression = dynamoMutation.getUpdateExpressionWithKey();
    String expectedCondition = dynamoMutation.getIfNotExistsCondition();
    Map<String, AttributeValue> expectedBindMap = dynamoMutation.getValueBindMapWithKey();

    // Act Assert
    assertThatCode(() -> handler.handle(put)).doesNotThrowAnyException();

    // Assert
    ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
    verify(client).updateItem(captor.capture());
    UpdateItemRequest actualRequest = captor.getValue();
    assertThat(actualRequest.key()).isEqualTo(expectedKeys);
    assertThat(actualRequest.updateExpression()).isEqualTo(updateExpression);
    assertThat(actualRequest.conditionExpression()).isEqualTo(expectedCondition);
    assertThat(actualRequest.expressionAttributeValues()).isEqualTo(expectedBindMap);
    assertThat(actualRequest.tableName()).isEqualTo(getFullTableName());
  }

  @Test
  public void handle_PutIfExistsGiven_ShouldCallUpdateItemWithCondition() {
    // Arrange
    when(client.updateItem(any(UpdateItemRequest.class))).thenReturn(updateResponse);
    Put put = preparePut().withCondition(new PutIfExists());
    DynamoMutation dynamoMutation = new DynamoMutation(put, metadata);
    Map<String, AttributeValue> expectedKeys = dynamoMutation.getKeyMap();
    String updateExpression = dynamoMutation.getUpdateExpression();
    String expectedCondition = dynamoMutation.getIfExistsCondition();
    Map<String, AttributeValue> expectedBindMap = dynamoMutation.getValueBindMap();

    // Act Assert
    assertThatCode(() -> handler.handle(put)).doesNotThrowAnyException();

    // Assert
    ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
    verify(client).updateItem(captor.capture());
    UpdateItemRequest actualRequest = captor.getValue();
    assertThat(actualRequest.key()).isEqualTo(expectedKeys);
    assertThat(actualRequest.updateExpression()).isEqualTo(updateExpression);
    assertThat(actualRequest.conditionExpression()).isEqualTo(expectedCondition);
    assertThat(actualRequest.expressionAttributeValues()).isEqualTo(expectedBindMap);
    assertThat(actualRequest.tableName()).isEqualTo(getFullTableName());
  }

  @Test
  public void handle_DynamoDbExceptionWithConditionalCheckFailed_ShouldThrowNoMutationException() {
    // Arrange
    ConditionalCheckFailedException toThrow = mock(ConditionalCheckFailedException.class);
    doThrow(toThrow).when(client).updateItem(any(UpdateItemRequest.class));

    Put put = preparePut().withCondition(new PutIfExists());

    // Act Assert
    assertThatThrownBy(() -> handler.handle(put)).isInstanceOf(NoMutationException.class);
  }
}
