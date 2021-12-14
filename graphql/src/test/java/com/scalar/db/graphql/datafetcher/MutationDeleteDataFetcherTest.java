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
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
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

public class MutationDeleteDataFetcherTest extends DataFetcherTestBase {
  private TableGraphQlModel storageTableGraphQlModel;
  private TableGraphQlModel transactionalTableGraphQlModel;
  private Map<String, Object> simpleDeleteArgument;
  private Delete simpleExpectedDelete;

  @Override
  public void doSetUp() throws Exception {
    // Arrange
    TableMetadata storageTableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.TEXT)
            .addColumn("c3", DataType.DOUBLE)
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

  private void prepareSimpleDelete() {
    // table1_delete(delete: {
    //   key: { c1: 1, c2: "A" },
    //   consistency: EVENTUAL
    // })
    simpleDeleteArgument = ImmutableMap.of("key", ImmutableMap.of("c1", 1, "c2", "A"));
    when(environment.getArgument("delete")).thenReturn(simpleDeleteArgument);

    simpleExpectedDelete =
        new Delete(new Key("c1", 1), new Key("c2", "A"))
            .forNamespace(ANY_NAMESPACE)
            .forTable(ANY_TABLE);
  }

  @Test
  public void get_ForStorageTable_ShouldUseStorage() throws Exception {
    // Arrange
    prepareSimpleDelete();
    MutationDeleteDataFetcher dataFetcher =
        spy(
            new MutationDeleteDataFetcher(
                storage, new DataFetcherHelper(storageTableGraphQlModel)));

    // Act
    dataFetcher.get(environment);

    // Assert
    verify(storage, times(1)).delete(simpleExpectedDelete);
    verify(transaction, never()).get(any());
  }

  @Test
  public void get_ForTransactionalTable_ShouldUseTransaction() throws Exception {
    // Arrange
    prepareSimpleDelete();
    MutationDeleteDataFetcher dataFetcher =
        spy(
            new MutationDeleteDataFetcher(
                storage, new DataFetcherHelper(transactionalTableGraphQlModel)));

    // Act
    dataFetcher.get(environment);

    // Assert
    verify(storage, never()).get(any());
    verify(transaction, times(1)).delete(simpleExpectedDelete);
  }

  private void testDeleteCommandIssued(Map<String, Object> deleteArgument, Delete expectedDelete)
      throws Exception {
    // Arrange
    when(environment.getArgument("delete")).thenReturn(deleteArgument);
    MutationDeleteDataFetcher dataFetcher =
        spy(
            new MutationDeleteDataFetcher(
                storage, new DataFetcherHelper(storageTableGraphQlModel)));

    // Act
    dataFetcher.get(environment);

    // Assert
    ArgumentCaptor<Delete> argument = ArgumentCaptor.forClass(Delete.class);
    verify(dataFetcher, times(1)).performDelete(eq(environment), argument.capture());
    assertThat(argument.getValue()).isEqualTo(expectedDelete);
  }

  @Test
  public void get_DeleteArgumentGiven_ShouldRunScalarDbDelete() throws Exception {
    // Arrange
    prepareSimpleDelete();

    // Act Assert
    testDeleteCommandIssued(simpleDeleteArgument, simpleExpectedDelete);
  }

  @Test
  public void get_DeleteArgumentWithConsistencyGiven_ShouldRunScalarDbDeleteWithConsistency()
      throws Exception {
    // Arrange
    prepareSimpleDelete();
    Map<String, Object> putArgumentWithConsistency = new HashMap<>(simpleDeleteArgument);
    putArgumentWithConsistency.put("consistency", "EVENTUAL");

    // Act Assert
    testDeleteCommandIssued(
        putArgumentWithConsistency, simpleExpectedDelete.withConsistency(Consistency.EVENTUAL));
  }

  @Test
  public void get_DeleteArgumentWithDeleteIfExistsGiven_ShouldRunScalarDbDeleteWithDeleteIfExists()
      throws Exception {
    // Arrange
    prepareSimpleDelete();
    Map<String, Object> deleteArgumentWithDeleteIfExists = new HashMap<>(simpleDeleteArgument);
    deleteArgumentWithDeleteIfExists.put("condition", ImmutableMap.of("type", "DeleteIfExists"));

    // Act Assert
    testDeleteCommandIssued(
        deleteArgumentWithDeleteIfExists, simpleExpectedDelete.withCondition(new DeleteIfExists()));
  }

  @Test
  public void get_DeleteArgumentWithDeleteIfGiven_ShouldRunScalarDbDeleteWithDeleteIf()
      throws Exception {
    // Arrange
    prepareSimpleDelete();
    Map<String, Object> deleteArgumentWithDeleteIf = new HashMap<>(simpleDeleteArgument);
    deleteArgumentWithDeleteIf.put(
        "condition",
        ImmutableMap.of(
            "type",
            "DeleteIf",
            "expressions",
            ImmutableList.of(
                ImmutableMap.of("name", "c2", "intValue", 1, "operator", "EQ"),
                ImmutableMap.of("name", "c3", "floatValue", 2.0F, "operator", "LTE"))));

    // Act Assert
    testDeleteCommandIssued(
        deleteArgumentWithDeleteIf,
        simpleExpectedDelete.withCondition(
            new DeleteIf(
                new ConditionalExpression("c2", new IntValue(1), Operator.EQ),
                new ConditionalExpression("c3", new FloatValue(2.0F), Operator.LTE))));
  }

  @Test
  public void
      get_DeleteArgumentWithDeleteIfWithNullConditionsGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    prepareSimpleDelete();
    Map<String, Object> deleteArgumentWithDeleteIf = new HashMap<>(simpleDeleteArgument);
    deleteArgumentWithDeleteIf.put("condition", ImmutableMap.of("type", "DeleteIf"));
    when(environment.getArgument("delete")).thenReturn(deleteArgumentWithDeleteIf);
    MutationDeleteDataFetcher dataFetcher =
        spy(
            new MutationDeleteDataFetcher(
                storage, new DataFetcherHelper(storageTableGraphQlModel)));

    // Act Assert
    assertThatThrownBy(() -> dataFetcher.get(environment))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void get_DeleteArgumentGiven_ShouldReturnTrue() throws Exception {
    // Arrange
    prepareSimpleDelete();
    MutationDeleteDataFetcher dataFetcher =
        new MutationDeleteDataFetcher(storage, new DataFetcherHelper(storageTableGraphQlModel));

    // Act
    DataFetcherResult<Boolean> result = dataFetcher.get(environment);

    // Assert
    assertThat(result.getData()).isTrue();
    assertThat(result.getErrors()).isEmpty();
  }

  @Test
  public void get_TransactionExceptionThrown_ShouldReturnFalseResultWithErrors() throws Exception {
    // Arrange
    prepareSimpleDelete();
    MutationDeleteDataFetcher dataFetcher =
        spy(new MutationDeleteDataFetcher(storage, new DataFetcherHelper(storageTableGraphQlModel)));
    TransactionException exception = new TransactionException("error");
    doThrow(exception)
        .when(dataFetcher)
        .performDelete(any(DataFetchingEnvironment.class), eq(simpleExpectedDelete));

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
