package com.scalar.db.graphql.datafetcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
import com.scalar.db.graphql.schema.TableGraphQlModel;
import com.scalar.db.io.DataType;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils;
import java.util.HashMap;
import java.util.List;
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
    // table1_delete(delete: [{
    //   key: { c1: 1, c2: "A" },
    //   consistency: EVENTUAL
    // }])
    simpleDeleteArgument = ImmutableMap.of("key", ImmutableMap.of("c1", 1, "c2", "A"));
    when(environment.getArgument("delete")).thenReturn(ImmutableList.of(simpleDeleteArgument));

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
        spy(new MutationDeleteDataFetcher(storage, storageTableGraphQlModel));

    // Act
    List<Map<String, Object>> result = dataFetcher.get(environment);

    // Assert
    verify(storage, times(1)).delete(simpleExpectedDelete);
    verify(transaction, never()).get(any());
  }

  @Test
  public void get_ForTransactionalTable_ShouldUseTransaction() throws Exception {
    // Arrange
    prepareSimpleDelete();
    MutationDeleteDataFetcher dataFetcher =
        spy(new MutationDeleteDataFetcher(storage, transactionalTableGraphQlModel));

    // Act
    List<Map<String, Object>> result = dataFetcher.get(environment);

    // Assert
    verify(storage, never()).get(any());
    verify(transaction, times(1))
        .delete(simpleExpectedDelete.withConsistency(Consistency.LINEARIZABLE));
  }

  private void testDeleteCommandIssued(Map<String, Object> deleteArgument, Delete expectedDelete)
      throws Exception {
    // Arrange
    when(environment.getArgument("delete")).thenReturn(ImmutableList.of(deleteArgument));
    MutationDeleteDataFetcher dataFetcher =
        spy(new MutationDeleteDataFetcher(storage, storageTableGraphQlModel));

    // Act
    List<Map<String, Object>> result = dataFetcher.get(environment);

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
      get_DeleteArgumentWithDeleteIfWithNullConditionsGiven_ShouldThrowIllegalStateException() {
    // Arrange
    prepareSimpleDelete();
    Map<String, Object> deleteArgumentWithDeleteIf = new HashMap<>(simpleDeleteArgument);
    deleteArgumentWithDeleteIf.put("condition", ImmutableMap.of("type", "DeleteIf"));
    when(environment.getArgument("delete"))
        .thenReturn(ImmutableList.of(deleteArgumentWithDeleteIf));
    MutationDeleteDataFetcher dataFetcher =
        spy(new MutationDeleteDataFetcher(storage, storageTableGraphQlModel));

    // Act Assert
    assertThatThrownBy(() -> dataFetcher.get(environment))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void get_SingleDeleteArgumentGiven_ShouldReturnResultAsList() throws Exception {
    // Arrange
    prepareSimpleDelete();
    MutationDeleteDataFetcher dataFetcher =
        new MutationDeleteDataFetcher(storage, storageTableGraphQlModel);

    // Act
    List<Map<String, Object>> result = dataFetcher.get(environment);

    // Assert
    assertThat(result).hasSize(1);
    Map<String, Object> object = result.get(0);
    assertThat(object).containsOnlyKeys("applied", "key");
    assertThat((Boolean) object.get("applied")).isTrue();
    assertThat((Map<String, Object>) object.get("key"))
        .containsOnly(entry("c1", 1), entry("c2", "A"));
  }

  @Test
  public void get_MultipleDeleteArgumentsGiven_ShouldReturnResultAsList() throws Exception {
    // Arrange
    prepareSimpleDelete();
    MutationDeleteDataFetcher dataFetcher =
        new MutationDeleteDataFetcher(storage, storageTableGraphQlModel);

    // table1_delete(delete: [{
    //   key: { c1: 1, c2: "A" },
    //   consistency: EVENTUAL
    // },
    // {
    //   key: { c1: 2, c2: "B" },
    //   consistency: EVENTUAL
    // }])
    List<Map<String, Object>> multipleDeleteArguments =
        ImmutableList.of(
            simpleDeleteArgument,
            ImmutableMap.of(
                "key",
                ImmutableMap.of("c1", 2, "c2", "B"),
                "values",
                ImmutableMap.of("c3", 3.0),
                "consistency",
                "EVENTUAL"));
    when(environment.getArgument("delete")).thenReturn(multipleDeleteArguments);

    // Act
    List<Map<String, Object>> result = dataFetcher.get(environment);

    // Assert
    assertThat(result).hasSize(2);
    Map<String, Object> object = result.get(0);
    assertThat(object).containsOnlyKeys("applied", "key");
    assertThat((Boolean) object.get("applied")).isTrue();
    assertThat((Map<String, Object>) object.get("key"))
        .containsOnly(entry("c1", 1), entry("c2", "A"));
    object = result.get(1);
    assertThat(object).containsOnlyKeys("applied", "key");
    assertThat((Boolean) object.get("applied")).isTrue();
    assertThat((Map<String, Object>) object.get("key"))
        .containsOnly(entry("c1", 2), entry("c2", "B"));
  }
}
