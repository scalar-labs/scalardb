package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import com.scalar.db.io.Key;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

public abstract class BatchHandlerTestBase {
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

  private BatchHandler handler;
  @Mock private DynamoDbClient client;
  @Mock private TableMetadataManager metadataManager;
  @Mock private TableMetadata metadata;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    handler = new BatchHandler(client, metadataManager, getNamespacePrefix());

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

  private Delete prepareDelete() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Delete(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  @Test
  public void handle_TooManyOperationsGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    List<Put> mutations = new ArrayList<>();
    IntStream.range(0, 101).forEach(i -> mutations.add(preparePut()));

    // Act Assert
    assertThatThrownBy(() -> handler.handle(mutations))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      handle_TransactionCanceledExceptionWithConditionCheckFailed_ShouldThrowNoMutationException() {
    TransactionCanceledException toThrow =
        TransactionCanceledException.builder()
            .cancellationReasons(
                CancellationReason.builder().code("ConditionalCheckFailed").build())
            .build();
    doThrow(toThrow).when(client).transactWriteItems(any(TransactWriteItemsRequest.class));

    Put put = preparePut().withCondition(new PutIfNotExists());
    Delete delete = prepareDelete().withCondition(new DeleteIfExists());

    // Act Assert
    assertThatThrownBy(() -> handler.handle(Arrays.asList(put, delete)))
        .isInstanceOf(NoMutationException.class);
  }

  @Test
  public void
      handle_TransactionCanceledExceptionWithTransactionConflict_ShouldThrowRetriableExecutionException() {
    TransactionCanceledException toThrow =
        TransactionCanceledException.builder()
            .cancellationReasons(
                CancellationReason.builder().code("TransactionConflict").build(),
                CancellationReason.builder().code("None").build())
            .build();
    doThrow(toThrow).when(client).transactWriteItems(any(TransactWriteItemsRequest.class));

    Put put = preparePut().withCondition(new PutIfNotExists());
    Delete delete = prepareDelete().withCondition(new DeleteIfExists());

    // Act Assert
    assertThatThrownBy(() -> handler.handle(Arrays.asList(put, delete)))
        .isInstanceOf(RetriableExecutionException.class);
  }

  @Test
  public void handle_DynamoDbExceptionThrown_ShouldThrowExecutionException() {
    // Arrange
    DynamoDbException toThrow = mock(DynamoDbException.class);
    doThrow(toThrow).when(client).transactWriteItems(any(TransactWriteItemsRequest.class));

    Put put1 = preparePut();
    Put put2 = preparePut();

    // Act Assert
    assertThatThrownBy(() -> handler.handle(Arrays.asList(put1, put2)))
        .isInstanceOf(ExecutionException.class)
        .hasCause(toThrow);
  }

  @Test
  public void handle_WithPutAndDelete_ShouldWorkProperly() throws ExecutionException {
    Put put = preparePut();
    Delete delete = prepareDelete();

    // Act Assert
    handler.handle(Arrays.asList(put, delete));

    ArgumentCaptor<TransactWriteItemsRequest> argument =
        ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
    verify(client).transactWriteItems(argument.capture());
    TransactWriteItemsRequest capturedRequest = argument.getValue();
    assertThat(capturedRequest.transactItems()).hasSize(2);
    assertThat(capturedRequest.transactItems().get(0).update().tableName())
        .isEqualTo(getFullTableName());
    assertThat(capturedRequest.transactItems().get(1).delete().tableName())
        .isEqualTo(getFullTableName());
  }
}
