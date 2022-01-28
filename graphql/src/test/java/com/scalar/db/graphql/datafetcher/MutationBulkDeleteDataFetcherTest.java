package com.scalar.db.graphql.datafetcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Delete;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.graphql.schema.TableGraphQlModel;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils;
import graphql.execution.AbortExecutionException;
import graphql.execution.DataFetcherResult;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

public class MutationBulkDeleteDataFetcherTest extends DataFetcherTestBase {
  private static final String COL1 = "c1";
  private static final String COL2 = "c2";
  private static final String COL3 = "c3";
  private static final String COL4 = "c4";
  private static final String COL5 = "c5";
  private static final String COL6 = "c5";

  private TableMetadata tableMetadata;
  private MutationBulkDeleteDataFetcher dataFetcher;
  private Delete expectedDelete;
  @Captor private ArgumentCaptor<List<Delete>> deleteListCaptor;

  @Override
  public void doSetUp() {
    // Arrange
    tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(COL1, DataType.INT)
            .addColumn(COL2, DataType.TEXT)
            .addColumn(COL3, DataType.FLOAT)
            .addColumn(COL4, DataType.DOUBLE)
            .addColumn(COL5, DataType.BIGINT)
            .addColumn(COL6, DataType.BOOLEAN)
            .addPartitionKey(COL1)
            .addClusteringKey(COL2)
            .build();
    dataFetcher =
        spy(
            new MutationBulkDeleteDataFetcher(
                storage,
                new DataFetcherHelper(
                    new TableGraphQlModel(ANY_NAMESPACE, ANY_TABLE, tableMetadata))));
  }

  private void prepareDeleteAndExpectedDelete() {
    // table1_bulkDelete(delete: [{
    //   key: { c1: 1, c2: "A" }
    // }])
    Map<String, Object> deleteInput = ImmutableMap.of("key", ImmutableMap.of(COL1, 1, COL2, "A"));
    when(environment.getArgument("delete")).thenReturn(ImmutableList.of(deleteInput));

    expectedDelete =
        new Delete(new Key(COL1, 1), new Key(COL2, "A"))
            .forNamespace(ANY_NAMESPACE)
            .forTable(ANY_TABLE);
  }

  @Test
  public void get_WhenTransactionNotStarted_ShouldUseStorage() throws Exception {
    // Arrange
    prepareDeleteAndExpectedDelete();

    // Act
    dataFetcher.get(environment);

    // Assert
    verify(storage, times(1)).delete(deleteListCaptor.capture());
    assertThat(deleteListCaptor.getValue()).containsExactly(expectedDelete);
    verify(transaction, never()).get(any());
  }

  @Test
  public void get_WhenTransactionStarted_ShouldUseTransaction() throws Exception {
    // Arrange
    prepareDeleteAndExpectedDelete();
    setTransactionStarted();

    // Act
    dataFetcher.get(environment);

    // Assert
    verify(storage, never()).get(any());
    verify(transaction, times(1)).delete(deleteListCaptor.capture());
    assertThat(deleteListCaptor.getValue()).containsExactly(expectedDelete);
  }

  @Test
  public void get_DeleteArgumentGiven_ShouldRunScalarDbDelete() throws Exception {
    // Arrange
    prepareDeleteAndExpectedDelete();
    doNothing().when(dataFetcher).performDelete(eq(environment), anyList());

    // Act
    dataFetcher.get(environment);

    // Assert
    verify(dataFetcher, times(1)).performDelete(eq(environment), deleteListCaptor.capture());
    assertThat(deleteListCaptor.getValue()).containsExactly(expectedDelete);
  }

  @Test
  public void get_WhenDeleteSucceeds_ShouldReturnTrue() throws Exception {
    // Arrange
    prepareDeleteAndExpectedDelete();
    doNothing().when(dataFetcher).performDelete(eq(environment), anyList());

    // Act
    DataFetcherResult<Boolean> result = dataFetcher.get(environment);

    // Assert
    assertThat(result.getData()).isTrue();
    assertThat(result.getErrors()).isEmpty();
  }

  @Test
  public void get_WhenDeleteFails_ShouldReturnFalseWithErrors() throws Exception {
    // Arrange
    prepareDeleteAndExpectedDelete();
    ExecutionException exception = new ExecutionException("error");
    doThrow(exception).when(dataFetcher).performDelete(eq(environment), anyList());

    // Act
    DataFetcherResult<Boolean> result = dataFetcher.get(environment);

    // Assert
    assertThat(result.getData()).isFalse();
    assertThatDataFetcherResultHasErrorForException(result, exception);
  }

  private void prepareTransactionalTable() {
    tableMetadata = ConsensusCommitUtils.buildTransactionalTableMetadata(tableMetadata);
    dataFetcher =
        new MutationBulkDeleteDataFetcher(
            storage,
            new DataFetcherHelper(new TableGraphQlModel(ANY_NAMESPACE, ANY_TABLE, tableMetadata)));
  }

  @Test
  public void
      performDelete_WhenTransactionalMetadataTableIsAccessedWithStorage_ShouldThrowException()
          throws Exception {
    // Arrange
    prepareTransactionalTable();
    List<Delete> deleteList = Collections.singletonList(new Delete(new Key(COL1, 1)));

    // Act Assert
    assertThatThrownBy(() -> dataFetcher.performDelete(environment, deleteList))
        .isInstanceOf(AbortExecutionException.class);
    verify(storage, never()).delete(deleteList);
    verify(transaction, never()).delete(deleteList);
  }

  @Test
  public void
      performDelete_WhenTransactionalMetadataTableIsAccessedWithTransaction_ShouldRunCommand()
          throws Exception {
    // Arrange
    prepareTransactionalTable();
    setTransactionStarted();
    List<Delete> deleteList = Collections.singletonList(new Delete(new Key(COL1, 1)));

    // Act
    dataFetcher.performDelete(environment, deleteList);

    // Assert
    verify(storage, never()).delete(deleteList);
    verify(transaction, times(1)).delete(deleteList);
  }
}
