package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Operation;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.Key;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

public abstract class DeleteStatementHandlerTestBase {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";

  private DeleteStatementHandler handler;
  @Mock private DynamoDbClient client;
  @Mock private TableMetadataManager metadataManager;
  @Mock private TableMetadata metadata;
  @Mock private DeleteItemResponse response;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    handler = new DeleteStatementHandler(client, metadataManager, getNamespacePrefix());

    when(metadataManager.getTableMetadata(any(Operation.class))).thenReturn(metadata);
    when(metadata.getPartitionKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_1)));
  }

  abstract Optional<String> getNamespacePrefix();

  private String getFullTableName() {
    return getNamespacePrefix().orElse("") + ANY_NAMESPACE_NAME + "." + ANY_TABLE_NAME;
  }

  private Delete prepareDelete() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Delete(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  @Test
  public void handle_DeleteWithoutConditionsGiven_ShouldCallDeleteItem() {
    // Arrange
    when(client.deleteItem(any(DeleteItemRequest.class))).thenReturn(response);
    Delete delete = prepareDelete();
    DynamoMutation dynamoMutation = new DynamoMutation(delete, metadata);

    // Act Assert
    assertThatCode(() -> handler.handle(delete)).doesNotThrowAnyException();

    // Assert
    ArgumentCaptor<DeleteItemRequest> captor = ArgumentCaptor.forClass(DeleteItemRequest.class);
    verify(client).deleteItem(captor.capture());
    DeleteItemRequest actualRequest = captor.getValue();
    assertThat(actualRequest.key()).isEqualTo(dynamoMutation.getKeyMap());
    assertThat(actualRequest.conditionExpression()).isNull();
    assertThat(actualRequest.tableName()).isEqualTo(getFullTableName());
  }

  @Test
  public void
      handle_DeleteWithoutConditionsDynamoDbExceptionThrown_ShouldThrowExecutionException() {
    // Arrange
    DynamoDbException toThrow = mock(DynamoDbException.class);
    doThrow(toThrow).when(client).deleteItem(any(DeleteItemRequest.class));

    Delete delete = prepareDelete();

    // Act Assert
    assertThatThrownBy(() -> handler.handle(delete))
        .isInstanceOf(ExecutionException.class)
        .hasCause(toThrow);
  }

  @Test
  public void handle_DeleteWithoutClusteringKeyGiven_ShouldCallDeleteItem() {
    // Arrange
    when(metadata.getClusteringKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_2)));
    when(client.deleteItem(any(DeleteItemRequest.class))).thenReturn(response);

    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Delete delete =
        new Delete(partitionKey).forNamespace(ANY_NAMESPACE_NAME).forTable(ANY_TABLE_NAME);

    DynamoMutation dynamoMutation = new DynamoMutation(delete, metadata);

    // Act Assert
    assertThatCode(() -> handler.handle(delete)).doesNotThrowAnyException();

    // Assert
    ArgumentCaptor<DeleteItemRequest> captor = ArgumentCaptor.forClass(DeleteItemRequest.class);
    verify(client).deleteItem(captor.capture());
    DeleteItemRequest actualRequest = captor.getValue();
    assertThat(actualRequest.key()).isEqualTo(dynamoMutation.getKeyMap());
    assertThat(actualRequest.conditionExpression()).isNull();
    assertThat(actualRequest.tableName()).isEqualTo(getFullTableName());
  }

  @Test
  public void handle_DeleteWithConditionsGiven_ShouldCallDeleteItem() {
    // Arrange
    when(metadata.getClusteringKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_2)));
    when(client.deleteItem(any(DeleteItemRequest.class))).thenReturn(response);

    Delete delete = prepareDelete().withCondition(new DeleteIfExists());

    DynamoMutation dynamoMutation = new DynamoMutation(delete, metadata);

    // Act Assert
    assertThatCode(() -> handler.handle(delete)).doesNotThrowAnyException();

    // Assert
    ArgumentCaptor<DeleteItemRequest> captor = ArgumentCaptor.forClass(DeleteItemRequest.class);
    verify(client).deleteItem(captor.capture());
    DeleteItemRequest actualRequest = captor.getValue();
    assertThat(actualRequest.key()).isEqualTo(dynamoMutation.getKeyMap());
    assertThat(actualRequest.conditionExpression())
        .isEqualTo(dynamoMutation.getIfExistsCondition());
    assertThat(actualRequest.tableName()).isEqualTo(getFullTableName());
  }

  @Test
  public void handle_DynamoDbExceptionWithConditionalCheckFailed_ShouldThrowNoMutationException() {
    // Arrange
    when(metadata.getClusteringKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_2)));
    when(client.deleteItem(any(DeleteItemRequest.class))).thenReturn(response);
    ConditionalCheckFailedException toThrow = mock(ConditionalCheckFailedException.class);
    doThrow(toThrow).when(client).deleteItem(any(DeleteItemRequest.class));

    Delete delete = prepareDelete().withCondition(new DeleteIfExists());

    // Act Assert
    assertThatThrownBy(() -> handler.handle(delete)).isInstanceOf(NoMutationException.class);
  }

  @Test
  public void handle_DeleteWithConditionDynamoDbExceptionThrown_ShouldThrowExecutionException() {
    // Arrange
    when(metadata.getClusteringKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_2)));
    when(client.deleteItem(any(DeleteItemRequest.class))).thenReturn(response);
    DynamoDbException toThrow = mock(DynamoDbException.class);
    doThrow(toThrow).when(client).deleteItem(any(DeleteItemRequest.class));

    Delete delete = prepareDelete().withCondition(new DeleteIfExists());

    // Act Assert
    assertThatThrownBy(() -> handler.handle(delete))
        .isInstanceOf(ExecutionException.class)
        .hasCause(toThrow);
  }
}
