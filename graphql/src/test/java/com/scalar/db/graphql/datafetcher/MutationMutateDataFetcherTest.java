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
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.graphql.schema.TableGraphQlModel;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils;
import graphql.execution.AbortExecutionException;
import graphql.execution.DataFetcherResult;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

public class MutationMutateDataFetcherTest extends DataFetcherTestBase {
  private static final String COL1 = "c1";
  private static final String COL2 = "c2";
  private static final String COL3 = "c3";
  private static final String COL4 = "c4";
  private static final String COL5 = "c5";
  private static final String COL6 = "c5";

  private TableMetadata tableMetadata;
  private MutationMutateDataFetcher dataFetcher;
  private Put expectedPut;
  private Delete expectedDelete;
  @Captor private ArgumentCaptor<List<Mutation>> mutationListCaptor;

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
            new MutationMutateDataFetcher(
                storage,
                new DataFetcherHelper(
                    new TableGraphQlModel(ANY_NAMESPACE, ANY_TABLE, tableMetadata))));
  }

  private void preparePutAndDelete() {
    // table1_mutate(
    //   put: [{
    //     key: { c1: 1, c2: "A" },
    //     values: { c3: 2.0 }
    //   }],
    //   delete: [{
    //     key: { c1: 2, c2: "B" }
    //   }]
    // )
    Map<String, Object> putInput =
        ImmutableMap.of(
            "key", ImmutableMap.of(COL1, 1, COL2, "A"), "values", ImmutableMap.of(COL3, 2.0F));
    when(environment.getArgument("put")).thenReturn(ImmutableList.of(putInput));
    Map<String, Object> deleteInput = ImmutableMap.of("key", ImmutableMap.of(COL1, 2, COL2, "B"));
    when(environment.getArgument("delete")).thenReturn(ImmutableList.of(deleteInput));

    expectedPut =
        new Put(new Key(COL1, 1), new Key(COL2, "A"))
            .withValue(COL3, 2.0F)
            .forNamespace(ANY_NAMESPACE)
            .forTable(ANY_TABLE);
    expectedDelete =
        new Delete(new Key(COL1, 2), new Key(COL2, "B"))
            .forNamespace(ANY_NAMESPACE)
            .forTable(ANY_TABLE);
  }

  @Test
  public void get_WhenTransactionNotStarted_ShouldUseStorage() throws Exception {
    // Arrange
    preparePutAndDelete();

    // Act
    dataFetcher.get(environment);

    // Assert
    verify(storage, times(1)).mutate(mutationListCaptor.capture());
    assertThat(mutationListCaptor.getValue()).containsExactly(expectedPut, expectedDelete);
    verify(transaction, never()).get(any());
  }

  @Test
  public void get_WhenTransactionStarted_ShouldUseTransaction() throws Exception {
    // Arrange
    preparePutAndDelete();
    setTransactionStarted();

    // Act
    dataFetcher.get(environment);

    // Assert
    verify(storage, never()).get(any());
    verify(transaction, times(1)).mutate(mutationListCaptor.capture());
    assertThat(mutationListCaptor.getValue()).containsExactly(expectedPut, expectedDelete);
  }

  @Test
  public void get_PutAndDeleteArgumentsGiven_ShouldRunScalarDbMutate() throws Exception {
    // Arrange
    preparePutAndDelete();
    doNothing().when(dataFetcher).performMutate(eq(environment), anyList());

    // Act
    dataFetcher.get(environment);

    // Assert
    verify(dataFetcher, times(1)).performMutate(eq(environment), mutationListCaptor.capture());
    assertThat(mutationListCaptor.getValue()).containsExactly(expectedPut, expectedDelete);
  }

  @Test
  public void get_WhenMutateSucceeds_ShouldReturnTrue() throws Exception {
    // Arrange
    preparePutAndDelete();
    doNothing().when(dataFetcher).performMutate(eq(environment), anyList());

    // Act
    DataFetcherResult<Boolean> result = dataFetcher.get(environment);

    // Assert
    assertThat(result.getData()).isTrue();
    assertThat(result.getErrors()).isEmpty();
  }

  @Test
  public void get_WhenMutateFails_ShouldReturnFalseWithErrors() throws Exception {
    // Arrange
    preparePutAndDelete();
    ExecutionException exception = new ExecutionException("error");
    doThrow(exception).when(dataFetcher).performMutate(eq(environment), anyList());

    // Act
    DataFetcherResult<Boolean> result = dataFetcher.get(environment);

    // Assert
    assertThat(result.getData()).isFalse();
    assertThatDataFetcherResultHasErrorForException(result, exception);
  }

  private void prepareTransactionalTable() {
    tableMetadata = ConsensusCommitUtils.buildTransactionalTableMetadata(tableMetadata);
    dataFetcher =
        new MutationMutateDataFetcher(
            storage,
            new DataFetcherHelper(new TableGraphQlModel(ANY_NAMESPACE, ANY_TABLE, tableMetadata)));
  }

  @Test
  public void
      performMutate_WhenTransactionalMetadataTableIsAccessedWithStorage_ShouldThrowException()
          throws Exception {
    // Arrange
    prepareTransactionalTable();
    List<Mutation> mutationList =
        Arrays.asList(new Put(new Key(COL1, 1)), new Delete(new Key(COL1, 1)));

    // Act Assert
    assertThatThrownBy(() -> dataFetcher.performMutate(environment, mutationList))
        .isInstanceOf(AbortExecutionException.class);
    verify(storage, never()).mutate(mutationList);
    verify(transaction, never()).mutate(mutationList);
  }

  @Test
  public void
      performMutate_WhenTransactionalMetadataTableIsAccessedWithTransaction_ShouldRunCommand()
          throws Exception {
    // Arrange
    prepareTransactionalTable();
    setTransactionStarted();
    List<Mutation> mutationList =
        Arrays.asList(new Put(new Key(COL1, 1)), new Delete(new Key(COL1, 1)));

    // Act
    dataFetcher.performMutate(environment, mutationList);

    // Assert
    verify(storage, never()).mutate(mutationList);
    verify(transaction, times(1)).mutate(mutationList);
  }
}
