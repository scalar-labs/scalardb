package com.scalar.db.graphql.datafetcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.graphql.schema.TableGraphQlModel;
import com.scalar.db.io.DataType;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils;
import graphql.execution.AbortExecutionException;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class MutationPutDataFetcherTest extends DataFetcherTestBase {
  private TableGraphQlModel storageTableGraphQlModel;
  private TableGraphQlModel transactionalTableGraphQlModel;
  private Map<String, Object> simplePutArgument;
  private Put simpleExpectedPut;

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
    // table1_put(put: {
    //   key: { c1: 1, c2: "A" },
    //   values: { c3: 2.0 }
    // })
    simplePutArgument =
        ImmutableMap.of(
            "key", ImmutableMap.of("c1", 1, "c2", "A"), "values", ImmutableMap.of("c3", 2.0F));
    when(environment.getArgument("put")).thenReturn(simplePutArgument);

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
    MutationPutDataFetcher dataFetcher =
        spy(new MutationPutDataFetcher(storage, new DataFetcherHelper(storageTableGraphQlModel)));

    // Act
    dataFetcher.get(environment);

    // Assert
    verify(storage, times(1)).put(simpleExpectedPut);
    verify(transaction, never()).get(any());
  }

  @Test
  public void get_ForTransactionalTable_ShouldUseTransaction() throws Exception {
    // Arrange
    prepareSimplePut();
    MutationPutDataFetcher dataFetcher =
        spy(
            new MutationPutDataFetcher(
                storage, new DataFetcherHelper(transactionalTableGraphQlModel)));

    // Act
    dataFetcher.get(environment);

    // Assert
    verify(storage, never()).get(any());
    verify(transaction, times(1)).put(simpleExpectedPut);
  }

  private void testPutCommandIssued(Map<String, Object> putArgument, Put expectedPut)
      throws Exception {
    // Arrange
    when(environment.getArgument("put")).thenReturn(putArgument);
    MutationPutDataFetcher dataFetcher =
        spy(new MutationPutDataFetcher(storage, new DataFetcherHelper(storageTableGraphQlModel)));

    // Act
    dataFetcher.get(environment);

    // Assert
    ArgumentCaptor<Put> argument = ArgumentCaptor.forClass(Put.class);
    verify(dataFetcher, times(1)).performPut(eq(environment), argument.capture());
    assertThat(argument.getValue()).isEqualTo(expectedPut);
  }

  @Test
  public void get_PutArgumentGiven_ShouldRunScalarDbPut() throws Exception {
    // Arrange
    prepareSimplePut();

    // Act Assert
    testPutCommandIssued(simplePutArgument, simpleExpectedPut);
  }

  @Test
  public void get_PutArgumentWithConsistencyGiven_ShouldRunScalarDbPutWithConsistency()
      throws Exception {
    // Arrange
    prepareSimplePut();
    Map<String, Object> putArgumentWithConsistency = new HashMap<>(simplePutArgument);
    putArgumentWithConsistency.put("consistency", "EVENTUAL");

    // Act Assert
    testPutCommandIssued(
        putArgumentWithConsistency, simpleExpectedPut.withConsistency(Consistency.EVENTUAL));
  }

  @Test
  public void get_PutArgumentWithPutIfExistsGiven_ShouldRunScalarDbPutWithPutIfExists()
      throws Exception {
    // Arrange
    prepareSimplePut();
    Map<String, Object> putArgumentWithPutIfExists = new HashMap<>(simplePutArgument);
    putArgumentWithPutIfExists.put("condition", ImmutableMap.of("type", "PutIfExists"));

    // Act Assert
    testPutCommandIssued(
        putArgumentWithPutIfExists, simpleExpectedPut.withCondition(new PutIfExists()));
  }

  @Test
  public void get_PutArgumentWithPutIfNotExistsGiven_ShouldRunScalarDbPutWithPutIfNotExists()
      throws Exception {
    // Arrange
    prepareSimplePut();
    Map<String, Object> putArgumentWithPutIfNotExists = new HashMap<>(simplePutArgument);
    putArgumentWithPutIfNotExists.put("condition", ImmutableMap.of("type", "PutIfNotExists"));

    // Act Assert
    testPutCommandIssued(
        putArgumentWithPutIfNotExists, simpleExpectedPut.withCondition(new PutIfNotExists()));
  }

  @Test
  public void get_PutArgumentWithPutIfGiven_ShouldRunScalarDbPutWithPutIf() throws Exception {
    // Arrange
    prepareSimplePut();
    Map<String, Object> putArgumentWithPutIf = new HashMap<>(simplePutArgument);
    putArgumentWithPutIf.put(
        "condition",
        ImmutableMap.of(
            "type",
            "PutIf",
            "expressions",
            ImmutableList.of(
                ImmutableMap.of("name", "c2", "intValue", 1, "operator", "EQ"),
                ImmutableMap.of("name", "c3", "floatValue", 2.0F, "operator", "LTE"))));

    // Act Assert
    testPutCommandIssued(
        putArgumentWithPutIf,
        simpleExpectedPut.withCondition(
            new PutIf(
                new ConditionalExpression("c2", new IntValue(1), Operator.EQ),
                new ConditionalExpression("c3", new FloatValue(2.0F), Operator.LTE))));
  }

  @Test
  public void
      get_PutArgumentWithPutIfWithNullConditionsGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    prepareSimplePut();
    Map<String, Object> putArgumentWithPutIf = new HashMap<>(simplePutArgument);
    putArgumentWithPutIf.put("condition", ImmutableMap.of("type", "PutIf"));
    when(environment.getArgument("put")).thenReturn(putArgumentWithPutIf);
    MutationPutDataFetcher dataFetcher =
        spy(new MutationPutDataFetcher(storage, new DataFetcherHelper(storageTableGraphQlModel)));

    // Act Assert
    assertThatThrownBy(() -> dataFetcher.get(environment))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void get_PutArgumentGiven_ShouldReturnTrueResult() throws Exception {
    // Arrange
    prepareSimplePut();
    MutationPutDataFetcher dataFetcher =
        new MutationPutDataFetcher(storage, new DataFetcherHelper(storageTableGraphQlModel));

    // Act
    DataFetcherResult<Boolean> result = dataFetcher.get(environment);

    // Assert
    assertThat(result.getData()).isTrue();
    assertThat(result.getErrors()).isEmpty();
  }

  @Test
  public void get_TransactionExceptionThrown_ShouldReturnFalseResultWithErrors() throws Exception {
    // Arrange
    prepareSimplePut();
    MutationPutDataFetcher dataFetcher =
        spy(new MutationPutDataFetcher(storage, new DataFetcherHelper(storageTableGraphQlModel)));
    TransactionException exception = new TransactionException("error");
    doThrow(exception)
        .when(dataFetcher)
        .performPut(any(DataFetchingEnvironment.class), eq(simpleExpectedPut));

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
