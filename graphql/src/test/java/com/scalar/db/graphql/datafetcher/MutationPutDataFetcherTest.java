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
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
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
    // table1_put(put: [{
    //   key: { c1: 1, c2: "A" },
    //   values: { c3: 2.0 }
    // }])
    simplePutArgument =
        ImmutableMap.of(
            "key", ImmutableMap.of("c1", 1, "c2", "A"), "values", ImmutableMap.of("c3", 2.0F));
    when(environment.getArgument("put")).thenReturn(ImmutableList.of(simplePutArgument));

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
        spy(new MutationPutDataFetcher(storage, storageTableGraphQlModel));

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
        spy(new MutationPutDataFetcher(storage, transactionalTableGraphQlModel));

    // Act
    dataFetcher.get(environment);

    // Assert
    verify(storage, never()).get(any());
    verify(transaction, times(1)).put(simpleExpectedPut);
  }

  private void testPutCommandIssued(Map<String, Object> putArgument, Put expectedPut)
      throws Exception {
    // Arrange
    when(environment.getArgument("put")).thenReturn(ImmutableList.of(putArgument));
    MutationPutDataFetcher dataFetcher =
        spy(new MutationPutDataFetcher(storage, storageTableGraphQlModel));

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
    when(environment.getArgument("put")).thenReturn(ImmutableList.of(putArgumentWithPutIf));
    MutationPutDataFetcher dataFetcher =
        spy(new MutationPutDataFetcher(storage, storageTableGraphQlModel));

    // Act Assert
    assertThatThrownBy(() -> dataFetcher.get(environment))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void get_SinglePutArgumentGiven_ShouldReturnResultAsList() throws Exception {
    // Arrange
    prepareSimplePut();
    MutationPutDataFetcher dataFetcher =
        new MutationPutDataFetcher(storage, storageTableGraphQlModel);

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
  public void get_MultiplePutArgumentsGiven_ShouldReturnResultAsList() throws Exception {
    // Arrange
    prepareSimplePut();
    MutationPutDataFetcher dataFetcher =
        new MutationPutDataFetcher(storage, storageTableGraphQlModel);

    // table1_put(put: [{
    //   key: { c1: 1, c2: "A" },
    //   values: { c3: 2.0 }
    // },
    // {
    //   key: { c1: 2, c2: "B" },
    //   values: { c3: 3.0, c4: 10.0, c5: 2147483648 c6: true }
    // }])
    List<Map<String, Object>> multiplePutArguments =
        ImmutableList.of(
            simplePutArgument,
            ImmutableMap.of(
                "key", ImmutableMap.of("c1", 2, "c2", "B"),
                "values",
                    ImmutableMap.of(
                        "c3", 3.0F, "c4", 10.0, "c5", Integer.MAX_VALUE + 1L, "c6", true)));
    when(environment.getArgument("put")).thenReturn(multiplePutArguments);

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
