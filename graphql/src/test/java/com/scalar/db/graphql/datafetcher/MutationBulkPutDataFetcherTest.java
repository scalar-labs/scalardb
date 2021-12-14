package com.scalar.db.graphql.datafetcher;

import static org.assertj.core.api.Assertions.assertThat;
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
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.graphql.schema.TableGraphQlModel;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils;
import graphql.execution.AbortExecutionException;
import graphql.execution.DataFetcherResult;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

public class MutationBulkPutDataFetcherTest extends DataFetcherTestBase {
  private TableGraphQlModel storageTableGraphQlModel;
  private TableGraphQlModel transactionalTableGraphQlModel;
  private Put simpleExpectedPut;
  @Captor private ArgumentCaptor<List<Put>> putListCaptor;

  @Override
  public void doSetUp() {
    // Arrange
    TableMetadata storageTableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.TEXT)
            .addColumn("c3", DataType.FLOAT)
            .addColumn("c4", DataType.DOUBLE)
            .addColumn("c5", DataType.BIGINT)
            .addColumn("c6", DataType.BOOLEAN)
            .addPartitionKey("c1")
            .addClusteringKey("c2")
            .build();
    storageTableGraphQlModel =
        new TableGraphQlModel(ANY_NAMESPACE, ANY_TABLE, storageTableMetadata);
    TableMetadata transactionalTableMetadata =
        ConsensusCommitUtils.buildTransactionalTableMetadata(storageTableMetadata);
    transactionalTableGraphQlModel =
        new TableGraphQlModel(ANY_NAMESPACE, ANY_TABLE, transactionalTableMetadata);
  }

  private void prepareSimplePut() {
    // table1_bulkPut(put: [{
    //   key: { c1: 1, c2: "A" },
    //   values: { c3: 2.0 }
    // }])
    Map<String, Object> simplePutInput =
        ImmutableMap.of(
            "key", ImmutableMap.of("c1", 1, "c2", "A"), "values", ImmutableMap.of("c3", 2.0F));
    when(environment.getArgument("put")).thenReturn(ImmutableList.of(simplePutInput));

    simpleExpectedPut =
        new Put(new Key("c1", 1), new Key("c2", "A"))
            .withValue("c3", 2.0F)
            .forNamespace(ANY_NAMESPACE)
            .forTable(ANY_TABLE);
  }

  @Test
  public void get_ForStorageTable_ShouldUseStorage() throws Exception {
    // Arrange
    prepareSimplePut();
    MutationBulkPutDataFetcher dataFetcher =
        new MutationBulkPutDataFetcher(storage, new DataFetcherHelper(storageTableGraphQlModel));

    // Act
    dataFetcher.get(environment);

    // Assert
    verify(storage, times(1)).put(putListCaptor.capture());
    assertThat(putListCaptor.getValue()).containsExactly(simpleExpectedPut);
    verify(transaction, never()).get(any());
  }

  @Test
  public void get_ForTransactionalTable_ShouldUseTransaction() throws Exception {
    // Arrange
    prepareSimplePut();
    MutationBulkPutDataFetcher dataFetcher =
        new MutationBulkPutDataFetcher(
            storage, new DataFetcherHelper(transactionalTableGraphQlModel));

    // Act
    dataFetcher.get(environment);

    // Assert
    verify(storage, never()).get(any());
    verify(transaction, times(1)).put(putListCaptor.capture());
    assertThat(putListCaptor.getValue()).containsExactly(simpleExpectedPut);
  }

  @Test
  public void get_PutInputListGiven_ShouldRunScalarDbPut() throws Exception {
    // Arrange
    prepareSimplePut();
    MutationBulkPutDataFetcher dataFetcher =
        spy(
            new MutationBulkPutDataFetcher(
                storage, new DataFetcherHelper(storageTableGraphQlModel)));
    doNothing().when(dataFetcher).performPut(eq(environment), anyList());

    // Act
    dataFetcher.get(environment);

    // Assert
    verify(dataFetcher, times(1)).performPut(eq(environment), putListCaptor.capture());
    assertThat(putListCaptor.getValue()).containsExactly(simpleExpectedPut);
  }

  @Test
  public void get_WhenPutSucceeds_ShouldReturnTrue() throws Exception {
    // Arrange
    prepareSimplePut();
    MutationBulkPutDataFetcher dataFetcher =
        spy(
            new MutationBulkPutDataFetcher(
                storage, new DataFetcherHelper(storageTableGraphQlModel)));
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
    prepareSimplePut();
    MutationBulkPutDataFetcher dataFetcher =
        spy(
            new MutationBulkPutDataFetcher(
                storage, new DataFetcherHelper(storageTableGraphQlModel)));
    TransactionException exception = new TransactionException("error");
    doThrow(exception).when(dataFetcher).performPut(eq(environment), anyList());

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
