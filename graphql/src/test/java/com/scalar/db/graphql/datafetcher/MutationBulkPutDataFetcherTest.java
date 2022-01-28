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
import com.scalar.db.api.Put;
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

public class MutationBulkPutDataFetcherTest extends DataFetcherTestBase {
  private static final String COL1 = "c1";
  private static final String COL2 = "c2";
  private static final String COL3 = "c3";
  private static final String COL4 = "c4";
  private static final String COL5 = "c5";
  private static final String COL6 = "c5";

  private TableMetadata tableMetadata;
  private MutationBulkPutDataFetcher dataFetcher;
  private Put expectedPut;
  @Captor private ArgumentCaptor<List<Put>> putListCaptor;

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
            new MutationBulkPutDataFetcher(
                storage,
                new DataFetcherHelper(
                    new TableGraphQlModel(ANY_NAMESPACE, ANY_TABLE, tableMetadata))));
  }

  private void preparePutInputAndExpectedPut() {
    // table1_bulkPut(put: [{
    //   key: { c1: 1, c2: "A" },
    //   values: { c3: 2.0 }
    // }])
    Map<String, Object> putInput =
        ImmutableMap.of(
            "key", ImmutableMap.of(COL1, 1, COL2, "A"), "values", ImmutableMap.of(COL3, 2.0F));
    when(environment.getArgument("put")).thenReturn(ImmutableList.of(putInput));

    expectedPut =
        new Put(new Key(COL1, 1), new Key(COL2, "A"))
            .withValue(COL3, 2.0F)
            .forNamespace(ANY_NAMESPACE)
            .forTable(ANY_TABLE);
  }

  @Test
  public void get_WhenTransactionNotStarted_ShouldUseStorage() throws Exception {
    // Arrange
    preparePutInputAndExpectedPut();

    // Act
    dataFetcher.get(environment);

    // Assert
    verify(storage, times(1)).put(putListCaptor.capture());
    assertThat(putListCaptor.getValue()).containsExactly(expectedPut);
    verify(transaction, never()).get(any());
  }

  @Test
  public void get_WhenTransactionStarted_ShouldUseTransaction() throws Exception {
    // Arrange
    preparePutInputAndExpectedPut();
    setTransactionStarted();

    // Act
    dataFetcher.get(environment);

    // Assert
    verify(storage, never()).get(any());
    verify(transaction, times(1)).put(putListCaptor.capture());
    assertThat(putListCaptor.getValue()).containsExactly(expectedPut);
  }

  @Test
  public void get_PutArgumentGiven_ShouldRunScalarDbPut() throws Exception {
    // Arrange
    preparePutInputAndExpectedPut();
    doNothing().when(dataFetcher).performPut(eq(environment), anyList());

    // Act
    dataFetcher.get(environment);

    // Assert
    verify(dataFetcher, times(1)).performPut(eq(environment), putListCaptor.capture());
    assertThat(putListCaptor.getValue()).containsExactly(expectedPut);
  }

  @Test
  public void get_WhenPutSucceeds_ShouldReturnTrue() throws Exception {
    // Arrange
    preparePutInputAndExpectedPut();
    doNothing().when(dataFetcher).performPut(eq(environment), anyList());

    // Act
    DataFetcherResult<Boolean> result = dataFetcher.get(environment);

    // Assert
    assertThat(result.getData()).isTrue();
    assertThat(result.getErrors()).isEmpty();
  }

  @Test
  public void get_WhenPutFails_ShouldReturnFalseWithErrors() throws Exception {
    // Arrange
    preparePutInputAndExpectedPut();
    ExecutionException exception = new ExecutionException("error");
    doThrow(exception).when(dataFetcher).performPut(eq(environment), anyList());

    // Act
    DataFetcherResult<Boolean> result = dataFetcher.get(environment);

    // Assert
    assertThat(result.getData()).isFalse();
    assertThatDataFetcherResultHasErrorForException(result, exception);
  }

  private void prepareTransactionalTable() {
    tableMetadata = ConsensusCommitUtils.buildTransactionalTableMetadata(tableMetadata);
    dataFetcher =
        new MutationBulkPutDataFetcher(
            storage,
            new DataFetcherHelper(new TableGraphQlModel(ANY_NAMESPACE, ANY_TABLE, tableMetadata)));
  }

  @Test
  public void performPut_WhenTransactionalMetadataTableIsAccessedWithStorage_ShouldThrowException()
      throws Exception {
    // Arrange
    prepareTransactionalTable();
    List<Put> putList = Collections.singletonList(new Put(new Key(COL1, 1)));

    // Act Assert
    assertThatThrownBy(() -> dataFetcher.performPut(environment, putList))
        .isInstanceOf(AbortExecutionException.class);
    verify(storage, never()).put(putList);
    verify(transaction, never()).put(putList);
  }

  @Test
  public void performPut_WhenTransactionalMetadataTableIsAccessedWithTransaction_ShouldRunCommand()
      throws Exception {
    // Arrange
    prepareTransactionalTable();
    setTransactionStarted();
    List<Put> putList = Collections.singletonList(new Put(new Key(COL1, 1)));

    // Act
    dataFetcher.performPut(environment, putList);

    // Assert
    verify(storage, never()).put(putList);
    verify(transaction, times(1)).put(putList);
  }
}
