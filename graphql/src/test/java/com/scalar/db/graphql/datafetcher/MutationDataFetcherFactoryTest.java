package com.scalar.db.graphql.datafetcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.graphql.schema.TableGraphQlModel;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils;
import graphql.execution.AbortExecutionException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class MutationDataFetcherFactoryTest extends DataFetcherTestBase {
  private static final String COL1 = "c1";
  private static final String COL2 = "c2";
  private static final String COL3 = "c3";
  private static final String COL4 = "c4";
  private static final String COL5 = "c5";
  private static final String COL6 = "c6";
  private static final int ANY_INTEGER = 1;
  private static final String ANY_STRING = "A";
  private static final float ANY_FLOAT = 2.0F;
  private static final double ANY_DOUBLE = 3.0;
  private static final long ANY_LONG = 4L;
  private static final boolean ANY_BOOLEAN = false;

  private MutationDataFetcherFactory dataFetcherFactory;
  private TableMetadata tableMetadata;

  private Map<String, Object> putInput;
  private Put expectedPut;
  private Map<String, Object> deleteInput;
  private Delete expectedDelete;

  @Before
  public void setUp() {
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
            .addPartitionKey(COL2)
            .addClusteringKey(COL3)
            .addClusteringKey(COL4)
            .build();
    dataFetcherFactory =
        new MutationDataFetcherFactory(
            new TableGraphQlModel(ANY_NAMESPACE, ANY_TABLE, tableMetadata), storage);
  }

  private void preparePutInputAndExpectedPut() {
    // table1_put(put: {
    //   key: { c1: 1, c2: "A", c3: 2.0, c4: 3.0 },
    //   values: { c5: 4, c6: false }
    // })
    putInput = new HashMap<>();
    putInput.put(
        "key",
        ImmutableMap.of(COL1, ANY_INTEGER, COL2, ANY_STRING, COL3, ANY_FLOAT, COL4, ANY_DOUBLE));
    putInput.put("values", ImmutableMap.of(COL5, ANY_LONG, COL6, ANY_BOOLEAN));

    expectedPut =
        new Put(
                new Key(new IntValue(COL1, ANY_INTEGER), new TextValue(COL2, ANY_STRING)),
                new Key(new FloatValue(COL3, ANY_FLOAT), new DoubleValue(COL4, ANY_DOUBLE)))
            .withValue(COL5, ANY_LONG)
            .withValue(COL6, ANY_BOOLEAN)
            .forNamespace(ANY_NAMESPACE)
            .forTable(ANY_TABLE);
  }

  @Test
  public void createPut_PutInputGiven_ShouldReturnPut() {
    // Arrange
    preparePutInputAndExpectedPut();

    // Act
    Put actual = dataFetcherFactory.createPut(putInput);

    // Assert
    assertThat(actual).isEqualTo(expectedPut);
  }

  @Test
  public void createPut_PutInputWithConsistencyGiven_ShouldReturnPutWithConsistency() {
    // Arrange
    preparePutInputAndExpectedPut();
    putInput.put("consistency", "EVENTUAL");
    expectedPut.withConsistency(Consistency.EVENTUAL);

    // Act
    Put actual = dataFetcherFactory.createPut(putInput);

    // Assert
    assertThat(actual).isEqualTo(expectedPut);
  }

  @Test
  public void createPut_PutInputWithPutIfExistsGiven_ShouldReturnPutWithPutIfExists() {
    // Arrange
    preparePutInputAndExpectedPut();
    putInput.put("condition", ImmutableMap.of("type", "PutIfExists"));
    expectedPut.withCondition(new PutIfExists());

    // Act
    Put actual = dataFetcherFactory.createPut(putInput);

    // Assert
    assertThat(actual).isEqualTo(expectedPut);
  }

  @Test
  public void createPut_PutInputWithPutIfNotExistsGiven_ShouldReturnPutWithPutIfNotExists() {
    // Arrange
    preparePutInputAndExpectedPut();
    putInput.put("condition", ImmutableMap.of("type", "PutIfNotExists"));
    expectedPut.withCondition(new PutIfNotExists());

    // Act
    Put actual = dataFetcherFactory.createPut(putInput);

    // Assert
    assertThat(actual).isEqualTo(expectedPut);
  }

  @Test
  public void createPut_PutInputWithPutIfGiven_ShouldReturnPutWithPutIf() {
    // Arrange
    preparePutInputAndExpectedPut();
    putInput.put(
        "condition",
        ImmutableMap.of(
            "type",
            "PutIf",
            "expressions",
            ImmutableList.of(
                ImmutableMap.of("name", COL2, "intValue", 1, "operator", "EQ"),
                ImmutableMap.of("name", COL3, "floatValue", 2.0F, "operator", "LTE"))));
    expectedPut.withCondition(
        new PutIf(
            new ConditionalExpression(COL2, new IntValue(1), Operator.EQ),
            new ConditionalExpression(COL3, new FloatValue(2.0F), Operator.LTE)));

    // Act
    Put actual = dataFetcherFactory.createPut(putInput);

    // Assert
    assertThat(actual).isEqualTo(expectedPut);
  }

  @Test
  public void
      createPut_PutInputWithPutIfWithNullConditionsGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    preparePutInputAndExpectedPut();
    putInput.put("condition", ImmutableMap.of("type", "PutIf"));

    // Act Assert
    assertThatThrownBy(() -> dataFetcherFactory.createPut(putInput))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private void prepareDeleteInputAndExpectedDelete() {
    // table1_delete(delete: {
    //   key: { c1: 1, c2: "A", c3: 2.0, c4: 3.0 },
    //   values: { c5: 10, c6: false }
    // })
    deleteInput = new HashMap<>();
    deleteInput.put(
        "key",
        ImmutableMap.of(COL1, ANY_INTEGER, COL2, ANY_STRING, COL3, ANY_FLOAT, COL4, ANY_DOUBLE));
    deleteInput.put("values", ImmutableMap.of(COL5, ANY_LONG, COL6, ANY_BOOLEAN));

    expectedDelete =
        new Delete(
                new Key(new IntValue(COL1, ANY_INTEGER), new TextValue(COL2, ANY_STRING)),
                new Key(new FloatValue(COL3, ANY_FLOAT), new DoubleValue(COL4, ANY_DOUBLE)))
            .forNamespace(ANY_NAMESPACE)
            .forTable(ANY_TABLE);
  }

  @Test
  public void createDelete_DeleteInputGiven_ShouldReturnDelete() {
    // Arrange
    prepareDeleteInputAndExpectedDelete();

    // Act
    Delete actual = dataFetcherFactory.createDelete(deleteInput);

    // Assert
    assertThat(actual).isEqualTo(expectedDelete);
  }

  @Test
  public void createDelete_DeleteInputWithConsistencyGiven_ShouldReturnDeleteWithConsistency() {
    // Arrange
    prepareDeleteInputAndExpectedDelete();
    deleteInput.put("consistency", "EVENTUAL");
    expectedDelete.withConsistency(Consistency.EVENTUAL);

    // Act
    Delete actual = dataFetcherFactory.createDelete(deleteInput);

    // Assert
    assertThat(actual).isEqualTo(expectedDelete);
  }

  @Test
  public void
      createDelete_DeleteInputWithDeleteIfExistsGiven_ShouldReturnDeleteWithDeleteIfExists() {
    // Arrange
    prepareDeleteInputAndExpectedDelete();
    deleteInput.put("condition", ImmutableMap.of("type", "DeleteIfExists"));
    expectedDelete.withCondition(new DeleteIfExists());

    // Act
    Delete actual = dataFetcherFactory.createDelete(deleteInput);

    // Assert
    assertThat(actual).isEqualTo(expectedDelete);
  }

  @Test
  public void createDelete_DeleteInputWithDeleteIfGiven_ShouldReturnDeleteWithDeleteIf() {
    // Arrange
    prepareDeleteInputAndExpectedDelete();
    deleteInput.put(
        "condition",
        ImmutableMap.of(
            "type",
            "DeleteIf",
            "expressions",
            ImmutableList.of(
                ImmutableMap.of("name", COL2, "intValue", 1, "operator", "EQ"),
                ImmutableMap.of("name", COL3, "floatValue", 2.0F, "operator", "LTE"))));
    expectedDelete.withCondition(
        new DeleteIf(
            new ConditionalExpression(COL2, new IntValue(1), Operator.EQ),
            new ConditionalExpression(COL3, new FloatValue(2.0F), Operator.LTE)));

    // Act
    Delete actual = dataFetcherFactory.createDelete(deleteInput);

    // Assert
    assertThat(actual).isEqualTo(expectedDelete);
  }

  @Test
  public void
      createDelete_DeleteInputWithDeleteIfWithNullConditionsGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    prepareDeleteInputAndExpectedDelete();
    deleteInput.put("condition", ImmutableMap.of("type", "DeleteIf"));

    // Act Assert
    assertThatThrownBy(() -> dataFetcherFactory.createDelete(deleteInput))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private void prepareTransactionalTable() {
    tableMetadata = ConsensusCommitUtils.buildTransactionalTableMetadata(tableMetadata);
    dataFetcherFactory =
        new MutationDataFetcherFactory(
            new TableGraphQlModel(ANY_NAMESPACE, ANY_TABLE, tableMetadata), storage);
  }

  @Test
  public void performPut_WhenTransactionNotStarted_ShouldUseStorage() throws Exception {
    // Arrange
    Put put = new Put(new Key(COL1, 1));

    // Act
    dataFetcherFactory.performPut(dataFetchingEnvironment, put);

    // Assert
    verify(storage, times(1)).put(put);
    verify(transaction, never()).get(any());
  }

  @Test
  public void performPut_WhenTransactionStarted_ShouldUseTransaction() throws Exception {
    // Arrange
    Put put = new Put(new Key(COL1, 1));
    setTransactionStarted();

    // Act
    dataFetcherFactory.performPut(dataFetchingEnvironment, put);

    // Assert
    verify(storage, never()).get(any());
    verify(transaction, times(1)).put(put);
  }

  @Test
  public void performPut_WhenTransactionalMetadataTableIsAccessedWithStorage_ShouldThrowException()
      throws Exception {
    // Arrange
    prepareTransactionalTable();
    Put put = new Put(new Key(COL1, 1));

    // Act Assert
    assertThatThrownBy(() -> dataFetcherFactory.performPut(dataFetchingEnvironment, put))
        .isInstanceOf(AbortExecutionException.class);
    verify(storage, never()).put(put);
    verify(transaction, never()).put(put);
  }

  @Test
  public void performPut_WhenTransactionalMetadataTableIsAccessedWithTransaction_ShouldRunCommand()
      throws Exception {
    // Arrange
    prepareTransactionalTable();
    setTransactionStarted();
    Put put = new Put(new Key(COL1, 1));

    // Act
    dataFetcherFactory.performPut(dataFetchingEnvironment, put);

    // Assert
    verify(storage, never()).put(put);
    verify(transaction, times(1)).put(put);
  }

  @Test
  public void
      performDelete_WhenTransactionalMetadataTableIsAccessedWithStorage_ShouldThrowException()
          throws Exception {
    // Arrange
    prepareTransactionalTable();
    Delete delete = new Delete(new Key(COL1, 1));

    // Act Assert
    assertThatThrownBy(() -> dataFetcherFactory.performDelete(dataFetchingEnvironment, delete))
        .isInstanceOf(AbortExecutionException.class);
    verify(storage, never()).delete(delete);
    verify(transaction, never()).delete(delete);
  }

  @Test
  public void
      performDelete_WhenTransactionalMetadataTableIsAccessedWithTransaction_ShouldRunCommand()
          throws Exception {
    // Arrange
    prepareTransactionalTable();
    setTransactionStarted();
    Delete delete = new Delete(new Key(COL1, 1));

    // Act
    dataFetcherFactory.performDelete(dataFetchingEnvironment, delete);

    // Assert
    verify(storage, never()).delete(delete);
    verify(transaction, times(1)).delete(delete);
  }

  @Test
  public void
      performPutList_WhenTransactionalMetadataTableIsAccessedWithStorage_ShouldThrowException()
          throws Exception {
    // Arrange
    prepareTransactionalTable();
    List<Put> putList = Collections.singletonList(new Put(new Key(COL1, 1)));

    // Act Assert
    assertThatThrownBy(() -> dataFetcherFactory.performPut(dataFetchingEnvironment, putList))
        .isInstanceOf(AbortExecutionException.class);
    verify(storage, never()).put(putList);
    verify(transaction, never()).put(putList);
  }

  @Test
  public void
      performPutList_WhenTransactionalMetadataTableIsAccessedWithTransaction_ShouldRunCommand()
          throws Exception {
    // Arrange
    prepareTransactionalTable();
    setTransactionStarted();
    List<Put> putList = Collections.singletonList(new Put(new Key(COL1, 1)));

    // Act
    dataFetcherFactory.performPut(dataFetchingEnvironment, putList);

    // Assert
    verify(storage, never()).put(putList);
    verify(transaction, times(1)).put(putList);
  }

  @Test
  public void
      performDeleteList_WhenTransactionalMetadataTableIsAccessedWithStorage_ShouldThrowException()
          throws Exception {
    // Arrange
    prepareTransactionalTable();
    List<Delete> deleteList = Collections.singletonList(new Delete(new Key(COL1, 1)));

    // Act Assert
    assertThatThrownBy(() -> dataFetcherFactory.performDelete(dataFetchingEnvironment, deleteList))
        .isInstanceOf(AbortExecutionException.class);
    verify(storage, never()).delete(deleteList);
    verify(transaction, never()).delete(deleteList);
  }

  @Test
  public void
      performDeleteList_WhenTransactionalMetadataTableIsAccessedWithTransaction_ShouldRunCommand()
          throws Exception {
    // Arrange
    prepareTransactionalTable();
    setTransactionStarted();
    List<Delete> deleteList = Collections.singletonList(new Delete(new Key(COL1, 1)));

    // Act
    dataFetcherFactory.performDelete(dataFetchingEnvironment, deleteList);

    // Assert
    verify(storage, never()).delete(deleteList);
    verify(transaction, times(1)).delete(deleteList);
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
    assertThatThrownBy(
            () -> dataFetcherFactory.performMutate(dataFetchingEnvironment, mutationList))
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
    dataFetcherFactory.performMutate(dataFetchingEnvironment, mutationList);

    // Assert
    verify(storage, never()).mutate(mutationList);
    verify(transaction, times(1)).mutate(mutationList);
  }
}
