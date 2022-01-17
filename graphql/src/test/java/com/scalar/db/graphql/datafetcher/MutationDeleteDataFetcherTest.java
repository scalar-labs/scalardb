package com.scalar.db.graphql.datafetcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Delete;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.graphql.schema.TableGraphQlModel;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils;
import graphql.execution.AbortExecutionException;
import graphql.execution.DataFetcherResult;
import java.util.Map;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

public class MutationDeleteDataFetcherTest extends DataFetcherTestBase {
  private static final String COL1 = "c1";
  private static final String COL2 = "c2";
  private static final String COL3 = "c3";

  private MutationDeleteDataFetcher dataFetcherForStorageTable;
  private MutationDeleteDataFetcher dataFetcherForTransactionalTable;
  private Delete expectedDelete;
  @Captor private ArgumentCaptor<Delete> deleteCaptor;

  @Override
  public void doSetUp() throws Exception {
    // Arrange
    TableMetadata storageTableMetadata =
        TableMetadata.newBuilder()
            .addColumn(COL1, DataType.INT)
            .addColumn(COL2, DataType.TEXT)
            .addColumn(COL3, DataType.FLOAT)
            .addPartitionKey(COL1)
            .addClusteringKey(COL2)
            .build();
    TableGraphQlModel storageTableGraphQlModel =
        new TableGraphQlModel(ANY_NAMESPACE, ANY_TABLE, storageTableMetadata);
    dataFetcherForStorageTable =
        new MutationDeleteDataFetcher(storage, new DataFetcherHelper(storageTableGraphQlModel));
    TableMetadata transactionalTableMetadata =
        ConsensusCommitUtils.buildTransactionalTableMetadata(storageTableMetadata);
    TableGraphQlModel transactionalTableGraphQlModel =
        new TableGraphQlModel(ANY_NAMESPACE, ANY_TABLE, transactionalTableMetadata);
    dataFetcherForTransactionalTable =
        new MutationDeleteDataFetcher(
            storage, new DataFetcherHelper(transactionalTableGraphQlModel));
  }

  private void prepareDeleteInputAndExpectedDelete() {
    // table1_delete(delete: {
    //   key: { c1: 1, c2: "A" },
    //   consistency: EVENTUAL
    // })
    Map<String, Object> deleteInput = ImmutableMap.of("key", ImmutableMap.of(COL1, 1, COL2, "A"));
    when(environment.getArgument("delete")).thenReturn(deleteInput);

    expectedDelete =
        new Delete(new Key(COL1, 1), new Key(COL2, "A"))
            .forNamespace(ANY_NAMESPACE)
            .forTable(ANY_TABLE);
  }

  @Test
  public void get_ForStorageTable_ShouldUseStorage() throws Exception {
    // Arrange
    prepareDeleteInputAndExpectedDelete();

    // Act
    dataFetcherForStorageTable.get(environment);

    // Assert
    verify(storage, times(1)).delete(expectedDelete);
    verify(transaction, never()).get(any());
  }

  @Test
  public void get_ForTransactionalTable_ShouldUseTransaction() throws Exception {
    // Arrange
    prepareDeleteInputAndExpectedDelete();

    // Act
    dataFetcherForTransactionalTable.get(environment);

    // Assert
    verify(storage, never()).get(any());
    verify(transaction, times(1)).delete(expectedDelete);
  }

  @Test
  public void get_DeleteArgumentGiven_ShouldRunScalarDbDelete() throws Exception {
    // Arrange
    prepareDeleteInputAndExpectedDelete();
    MutationDeleteDataFetcher dataFetcher = spy(dataFetcherForStorageTable);
    doNothing().when(dataFetcher).performDelete(eq(environment), any(Delete.class));

    // Act
    dataFetcher.get(environment);

    // Assert
    verify(dataFetcher, times(1)).performDelete(eq(environment), deleteCaptor.capture());
    assertThat(deleteCaptor.getValue()).isEqualTo(expectedDelete);
  }

  @Test
  public void get_WhenDeleteSucceeds_ShouldReturnTrue() throws Exception {
    // Arrange
    prepareDeleteInputAndExpectedDelete();
    MutationDeleteDataFetcher dataFetcher = spy(dataFetcherForStorageTable);
    doNothing().when(dataFetcher).performDelete(eq(environment), any(Delete.class));

    // Act
    DataFetcherResult<Boolean> result = dataFetcher.get(environment);

    // Assert
    assertThat(result.getData()).isTrue();
    assertThat(result.getErrors()).isEmpty();
  }

  @Test
  public void get_WhenDeleteFails_ShouldReturnFalseWithErrors() throws Exception {
    // Arrange
    prepareDeleteInputAndExpectedDelete();
    MutationDeleteDataFetcher dataFetcher = spy(dataFetcherForStorageTable);
    TransactionException exception = new TransactionException("error");
    doThrow(exception).when(dataFetcher).performDelete(eq(environment), any(Delete.class));

    // Act
    DataFetcherResult<Boolean> result = dataFetcher.get(environment);

    // Assert
    assertThat(result.getData()).isFalse();
    assertThat(result.getErrors())
        .hasSize(1)
        .element(0)
        .isInstanceOf(AbortExecutionException.class);
    assertThat(((AbortExecutionException) result.getErrors().get(0)).getCause())
        .isSameAs(exception);
  }
}
