package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.api.ConditionalExpression.Operator;
import static com.scalar.db.transaction.consensuscommit.Attribute.ID;
import static com.scalar.db.transaction.consensuscommit.Attribute.STATE;
import static com.scalar.db.transaction.consensuscommit.Attribute.VERSION;
import static com.scalar.db.transaction.consensuscommit.Attribute.toIdValue;
import static com.scalar.db.transaction.consensuscommit.Attribute.toVersionValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteBuilder;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.util.ScalarDbUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class PrepareMutationComposerTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_ID_1 = "id1";
  private static final String ANY_ID_2 = "id2";
  private static final String ANY_ID_3 = "id3";
  private static final long ANY_TIME_1 = 100;
  private static final long ANY_TIME_2 = 200;
  private static final long ANY_TIME_3 = 300;
  private static final long ANY_TIME_4 = 400;
  private static final long ANY_TIME_5 = 500;
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_NAME_WITH_BEFORE_PREFIX = "before_x";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final int ANY_INT_1 = 100;
  private static final int ANY_INT_2 = 200;
  private static final int ANY_INT_3 = 300;
  private static final TableMetadata TABLE_METADATA =
      ConsensusCommitUtils.buildTransactionTableMetadata(
          TableMetadata.newBuilder()
              .addColumn(ANY_NAME_1, DataType.TEXT)
              .addColumn(ANY_NAME_2, DataType.TEXT)
              .addColumn(ANY_NAME_3, DataType.INT)
              .addColumn(ANY_NAME_WITH_BEFORE_PREFIX, DataType.INT)
              .addPartitionKey(ANY_NAME_1)
              .addClusteringKey(ANY_NAME_2)
              .build());

  @Mock private TransactionTableMetadataManager tableMetadataManager;

  private PrepareMutationComposer composer;
  private @Mock ConditionalExpression condExpression1;
  private @Mock ConditionalExpression condExpression2;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    composer = new PrepareMutationComposer(ANY_ID_3, ANY_TIME_5, tableMetadataManager);

    when(tableMetadataManager.getTransactionTableMetadata(any(Operation.class)))
        .thenReturn(new TransactionTableMetadata(TABLE_METADATA));
  }

  private PutBuilder.Buildable preparePut() {
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
        .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
        .intValue(ANY_NAME_3, ANY_INT_3)
        .intValue(ANY_NAME_WITH_BEFORE_PREFIX, ANY_INT_3);
  }

  private DeleteBuilder.Buildable prepareDelete() {
    return Delete.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
        .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2));
  }

  private Get prepareGet() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Get(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private Scan prepareScan() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    return Scan.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .build();
  }

  private TransactionResult prepareResult() {
    ImmutableMap<String, Column<?>> columns =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1))
            .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_2))
            .put(ANY_NAME_3, IntColumn.of(ANY_NAME_3, ANY_INT_2))
            .put(ANY_NAME_WITH_BEFORE_PREFIX, IntColumn.of(ANY_NAME_WITH_BEFORE_PREFIX, ANY_INT_2))
            .put(Attribute.ID, ScalarDbUtils.toColumn(Attribute.toIdValue(ANY_ID_2)))
            .put(
                Attribute.PREPARED_AT,
                ScalarDbUtils.toColumn(Attribute.toPreparedAtValue(ANY_TIME_3)))
            .put(
                Attribute.COMMITTED_AT,
                ScalarDbUtils.toColumn(Attribute.toCommittedAtValue(ANY_TIME_4)))
            .put(
                Attribute.STATE,
                ScalarDbUtils.toColumn(Attribute.toStateValue(TransactionState.COMMITTED)))
            .put(Attribute.VERSION, ScalarDbUtils.toColumn(Attribute.toVersionValue(2)))
            .put(
                Attribute.BEFORE_PREFIX + ANY_NAME_3,
                IntColumn.of(Attribute.BEFORE_PREFIX + ANY_NAME_3, ANY_INT_1))
            .put(
                Attribute.BEFORE_PREFIX + ANY_NAME_WITH_BEFORE_PREFIX,
                IntColumn.of(Attribute.BEFORE_PREFIX + ANY_NAME_WITH_BEFORE_PREFIX, ANY_INT_1))
            .put(Attribute.BEFORE_ID, ScalarDbUtils.toColumn(Attribute.toBeforeIdValue(ANY_ID_1)))
            .put(
                Attribute.BEFORE_PREPARED_AT,
                ScalarDbUtils.toColumn(Attribute.toBeforePreparedAtValue(ANY_TIME_1)))
            .put(
                Attribute.BEFORE_COMMITTED_AT,
                ScalarDbUtils.toColumn(Attribute.toBeforeCommittedAtValue(ANY_TIME_2)))
            .put(
                Attribute.BEFORE_STATE,
                ScalarDbUtils.toColumn(Attribute.toBeforeStateValue(TransactionState.COMMITTED)))
            .put(
                Attribute.BEFORE_VERSION, ScalarDbUtils.toColumn(Attribute.toBeforeVersionValue(1)))
            .build();
    return new TransactionResult(new ResultImpl(columns, TABLE_METADATA));
  }

  @Test
  public void add_PutAndResultGiven_ShouldComposePutWithPutIfCondition() throws ExecutionException {
    // Arrange
    Put put = preparePut().build();
    TransactionResult result = prepareResult();

    // Act
    composer.add(put, result);

    // Assert
    Put actual = (Put) composer.get().get(0);
    put.withConsistency(Consistency.LINEARIZABLE);
    put.withCondition(
        new PutIf(
            new ConditionalExpression(VERSION, toVersionValue(2), Operator.EQ),
            new ConditionalExpression(ID, toIdValue(ANY_ID_2), Operator.EQ)));
    put.withValue(Attribute.toPreparedAtValue(ANY_TIME_5));
    put.withValue(Attribute.toIdValue(ANY_ID_3));
    put.withValue(Attribute.toStateValue(TransactionState.PREPARED));
    put.withValue(Attribute.toVersionValue(3));
    put.withValue(Attribute.toBeforePreparedAtValue(ANY_TIME_3));
    put.withValue(Attribute.toBeforeCommittedAtValue(ANY_TIME_4));
    put.withValue(Attribute.toBeforeIdValue(ANY_ID_2));
    put.withValue(Attribute.toBeforeStateValue(TransactionState.COMMITTED));
    put.withValue(Attribute.toBeforeVersionValue(2));
    put.withValue(Attribute.BEFORE_PREFIX + ANY_NAME_3, ANY_INT_2);
    put.withValue(Attribute.BEFORE_PREFIX + ANY_NAME_WITH_BEFORE_PREFIX, ANY_INT_2);
    assertThat(actual).isEqualTo(put);
  }

  @Test
  public void add_PutAndNullResultGiven_ShouldComposePutWithPutIfNotExistsCondition()
      throws ExecutionException {
    // Arrange
    Put put = preparePut().build();

    // Act
    composer.add(put, null);

    // Assert
    Put actual = (Put) composer.get().get(0);
    put.withConsistency(Consistency.LINEARIZABLE);
    put.withCondition(new PutIfNotExists());
    put.withValue(Attribute.toPreparedAtValue(ANY_TIME_5));
    put.withValue(Attribute.toIdValue(ANY_ID_3));
    put.withValue(Attribute.toStateValue(TransactionState.PREPARED));
    put.withValue(Attribute.toVersionValue(1));
    assertThat(actual).isEqualTo(put);
  }

  @Test
  public void add_PutWithPutIfConditionAndResultGiven_ShouldComposePutWithPutIfCondition()
      throws ExecutionException {
    // Arrange
    PutIf putIf = mock(PutIf.class);
    when(putIf.getExpressions()).thenReturn(ImmutableList.of(condExpression1, condExpression2));
    Put put = preparePut().condition(putIf).build();
    TransactionResult result = prepareResult();

    // Act
    composer.add(put, result);

    // Assert
    Put actual = (Put) composer.get().get(0);
    assertThat(actual.forNamespace()).get().isEqualTo(ANY_NAMESPACE_NAME);
    assertThat(actual.forTable()).get().isEqualTo(ANY_TABLE_NAME);
    assertThat(actual.getPartitionKey().equals(Key.ofText(ANY_NAME_1, ANY_TEXT_1))).isTrue();
    assertThat(actual.getClusteringKey()).get().isEqualTo(Key.ofText(ANY_NAME_2, ANY_TEXT_2));
    assertThat(actual.getIntValue(ANY_NAME_3)).isEqualTo(ANY_INT_3);
    assertThat(actual.getIntValue(ANY_NAME_WITH_BEFORE_PREFIX)).isEqualTo(ANY_INT_3);
    assertThat(actual.getConsistency()).isEqualTo(Consistency.LINEARIZABLE);
    assertThat(actual.getCondition())
        .get()
        .isInstanceOf(PutIf.class)
        .satisfies(
            c ->
                assertThat(c.getExpressions())
                    .containsExactly(
                        ConditionBuilder.column(VERSION).isEqualToInt(2),
                        ConditionBuilder.column(ID).isEqualToText(ANY_ID_2),
                        condExpression1,
                        condExpression2));
    assertThat(actual.getTextValue(ID)).isEqualTo(ANY_ID_3);
    assertThat(actual.getIntValue(STATE)).isEqualTo(TransactionState.PREPARED.get());
    assertThat(actual.getIntValue(VERSION)).isEqualTo(3);
    assertThat(actual.getBigIntValue(Attribute.PREPARED_AT)).isEqualTo(ANY_TIME_5);
    assertThat(actual.getBigIntValue(Attribute.BEFORE_PREPARED_AT)).isEqualTo(ANY_TIME_3);
    assertThat(actual.getBigIntValue(Attribute.BEFORE_COMMITTED_AT)).isEqualTo(ANY_TIME_4);
    assertThat(actual.getTextValue(Attribute.BEFORE_ID)).isEqualTo(ANY_ID_2);
    assertThat(actual.getIntValue(Attribute.BEFORE_STATE))
        .isEqualTo(TransactionState.COMMITTED.get());
    assertThat(actual.getIntValue(Attribute.BEFORE_VERSION)).isEqualTo(2);
    assertThat(actual.getIntValue(Attribute.BEFORE_PREFIX + ANY_NAME_3)).isEqualTo(ANY_INT_2);
    assertThat(actual.getIntValue(Attribute.BEFORE_PREFIX + ANY_NAME_WITH_BEFORE_PREFIX))
        .isEqualTo(ANY_INT_2);
  }

  @Test
  public void add_PutWithPutIfNotExistsConditionAndResultGiven_ShouldThrowNoMutationException() {
    // Arrange
    PutIfNotExists putIfNotExists = mock(PutIfNotExists.class);
    Put put = preparePut().condition(putIfNotExists).build();
    TransactionResult result = prepareResult();

    // Act Assert
    Assertions.assertThatThrownBy(() -> composer.add(put, result))
        .isInstanceOf(NoMutationException.class);
  }

  @Test
  public void add_PutWithPutIfExistsConditionAndResultGiven_ShouldComposePutWithPutIfCondition()
      throws ExecutionException {
    // Arrange
    PutIfExists putIfExists = mock(PutIfExists.class);
    Put put = preparePut().condition(putIfExists).build();
    TransactionResult result = prepareResult();

    // Act
    composer.add(put, result);

    // Assert
    Put actual = (Put) composer.get().get(0);
    assertThat(actual.forNamespace()).get().isEqualTo(ANY_NAMESPACE_NAME);
    assertThat(actual.forTable()).get().isEqualTo(ANY_TABLE_NAME);
    assertThat(actual.getPartitionKey().equals(Key.ofText(ANY_NAME_1, ANY_TEXT_1))).isTrue();
    assertThat(actual.getClusteringKey()).get().isEqualTo(Key.ofText(ANY_NAME_2, ANY_TEXT_2));
    assertThat(actual.getIntValue(ANY_NAME_3)).isEqualTo(ANY_INT_3);
    assertThat(actual.getIntValue(ANY_NAME_WITH_BEFORE_PREFIX)).isEqualTo(ANY_INT_3);
    assertThat(actual.getConsistency()).isEqualTo(Consistency.LINEARIZABLE);
    assertThat(actual.getCondition())
        .get()
        .isInstanceOf(PutIf.class)
        .satisfies(
            c ->
                assertThat(c.getExpressions())
                    .containsExactly(
                        ConditionBuilder.column(VERSION).isEqualToInt(2),
                        ConditionBuilder.column(ID).isEqualToText(ANY_ID_2)));
    assertThat(actual.getTextValue(ID)).isEqualTo(ANY_ID_3);
    assertThat(actual.getIntValue(STATE)).isEqualTo(TransactionState.PREPARED.get());
    assertThat(actual.getIntValue(VERSION)).isEqualTo(3);
    assertThat(actual.getBigIntValue(Attribute.PREPARED_AT)).isEqualTo(ANY_TIME_5);
    assertThat(actual.getBigIntValue(Attribute.BEFORE_PREPARED_AT)).isEqualTo(ANY_TIME_3);
    assertThat(actual.getBigIntValue(Attribute.BEFORE_COMMITTED_AT)).isEqualTo(ANY_TIME_4);
    assertThat(actual.getTextValue(Attribute.BEFORE_ID)).isEqualTo(ANY_ID_2);
    assertThat(actual.getIntValue(Attribute.BEFORE_STATE))
        .isEqualTo(TransactionState.COMMITTED.get());
    assertThat(actual.getIntValue(Attribute.BEFORE_VERSION)).isEqualTo(2);
    assertThat(actual.getIntValue(Attribute.BEFORE_PREFIX + ANY_NAME_3)).isEqualTo(ANY_INT_2);
    assertThat(actual.getIntValue(Attribute.BEFORE_PREFIX + ANY_NAME_WITH_BEFORE_PREFIX))
        .isEqualTo(ANY_INT_2);
  }

  @Test
  public void add_PutWithPutIfConditionAndNullResultGiven_ShouldThrowNoMutationException() {
    // Arrange
    PutIf putIf = mock(PutIf.class);
    Put put = preparePut().condition(putIf).build();

    // Act Assert
    assertThatThrownBy(() -> composer.add(put, null)).isInstanceOf(NoMutationException.class);
  }

  @Test
  public void add_PutWithPutIfExistsConditionAndNullResultGiven_ShouldThrowNoMutationException() {
    // Arrange
    PutIfExists putIfExists = mock(PutIfExists.class);
    Put put = preparePut().condition(putIfExists).build();

    // Act Assert
    assertThatThrownBy(() -> composer.add(put, null)).isInstanceOf(NoMutationException.class);
  }

  @Test
  public void
      add_PutWithPutIfNotExistsAndNullResultGiven_ShouldComposePutWithPutIfNotExistsCondition()
          throws ExecutionException {
    // Arrange
    PutIfNotExists putIfNotExists = mock(PutIfNotExists.class);
    Put put = preparePut().condition(putIfNotExists).build();

    // Act
    composer.add(put, null);

    // Assert
    Put actual = (Put) composer.get().get(0);
    assertThat(actual.forNamespace()).get().isEqualTo(ANY_NAMESPACE_NAME);
    assertThat(actual.forTable()).get().isEqualTo(ANY_TABLE_NAME);
    assertThat(actual.getPartitionKey().equals(Key.ofText(ANY_NAME_1, ANY_TEXT_1))).isTrue();
    assertThat(actual.getClusteringKey()).get().isEqualTo(Key.ofText(ANY_NAME_2, ANY_TEXT_2));
    assertThat(actual.getIntValue(ANY_NAME_3)).isEqualTo(ANY_INT_3);
    assertThat(actual.getIntValue(ANY_NAME_WITH_BEFORE_PREFIX)).isEqualTo(ANY_INT_3);
    assertThat(actual.getConsistency()).isEqualTo(Consistency.LINEARIZABLE);
    assertThat(put.getCondition()).get().isInstanceOf(PutIfNotExists.class);
    assertThat(actual.getBigIntValue(Attribute.PREPARED_AT)).isEqualTo(ANY_TIME_5);
    assertThat(actual.getTextValue(Attribute.ID)).isEqualTo(ANY_ID_3);
    assertThat(actual.getIntValue(STATE)).isEqualTo(TransactionState.PREPARED.get());
    assertThat(actual.getIntValue(VERSION)).isEqualTo(1);
  }

  @Test
  public void add_DeleteAndResultGiven_ShouldComposePutWithPutIfCondition()
      throws ExecutionException {
    // Arrange
    Delete delete = prepareDelete().build();
    TransactionResult result = prepareResult();

    // Act
    composer.add(delete, result);

    // Assert
    Put actual = (Put) composer.get().get(0);
    Put expected =
        new Put(delete.getPartitionKey(), delete.getClusteringKey().orElse(null))
            .forNamespace(delete.forNamespace().get())
            .forTable(delete.forTable().get());
    expected.withConsistency(Consistency.LINEARIZABLE);
    expected.withCondition(
        new PutIf(
            new ConditionalExpression(VERSION, toVersionValue(2), Operator.EQ),
            new ConditionalExpression(ID, toIdValue(ANY_ID_2), Operator.EQ)));
    expected.withValue(Attribute.toPreparedAtValue(ANY_TIME_5));
    expected.withValue(Attribute.toIdValue(ANY_ID_3));
    expected.withValue(Attribute.toStateValue(TransactionState.DELETED));
    expected.withValue(Attribute.toVersionValue(3));
    expected.withValue(Attribute.toBeforePreparedAtValue(ANY_TIME_3));
    expected.withValue(Attribute.toBeforeCommittedAtValue(ANY_TIME_4));
    expected.withValue(Attribute.toBeforeIdValue(ANY_ID_2));
    expected.withValue(Attribute.toBeforeStateValue(TransactionState.COMMITTED));
    expected.withValue(Attribute.toBeforeVersionValue(2));
    expected.withValue(Attribute.BEFORE_PREFIX + ANY_NAME_3, ANY_INT_2);
    expected.withValue(Attribute.BEFORE_PREFIX + ANY_NAME_WITH_BEFORE_PREFIX, ANY_INT_2);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void add_DeleteAndNullResultGiven_ShouldComposePutWithPutIfNotExistsCondition()
      throws ExecutionException {
    // Arrange
    Delete delete = prepareDelete().build();

    // Act
    composer.add(delete, null);

    // Assert
    Put actual = (Put) composer.get().get(0);
    Put expected =
        new Put(delete.getPartitionKey(), delete.getClusteringKey().orElse(null))
            .forNamespace(delete.forNamespace().get())
            .forTable(delete.forTable().get());
    expected.withConsistency(Consistency.LINEARIZABLE);
    expected.withCondition(new PutIfNotExists());
    expected.withValue(Attribute.toPreparedAtValue(ANY_TIME_5));
    expected.withValue(Attribute.toIdValue(ANY_ID_3));
    expected.withValue(Attribute.toStateValue(TransactionState.DELETED));
    expected.withValue(Attribute.toVersionValue(1));
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void add_DeleteWithDeleteIfConditionAndResultGiven_ShouldComposePutWithPutIfCondition()
      throws ExecutionException {
    // Arrange
    DeleteIf deleteIf = mock(DeleteIf.class);
    when(deleteIf.getExpressions()).thenReturn(ImmutableList.of(condExpression1, condExpression2));
    Delete delete = prepareDelete().condition(deleteIf).build();
    TransactionResult result = prepareResult();

    // Act
    composer.add(delete, result);

    // Assert
    Put actual = (Put) composer.get().get(0);
    assertThat(actual.forNamespace()).get().isEqualTo(ANY_NAMESPACE_NAME);
    assertThat(actual.forTable()).get().isEqualTo(ANY_TABLE_NAME);
    assertThat(actual.getPartitionKey().equals(Key.ofText(ANY_NAME_1, ANY_TEXT_1))).isTrue();
    assertThat(actual.getClusteringKey()).get().isEqualTo(Key.ofText(ANY_NAME_2, ANY_TEXT_2));
    assertThat(actual.getConsistency()).isEqualTo(Consistency.LINEARIZABLE);
    assertThat(actual.getCondition())
        .get()
        .isInstanceOf(PutIf.class)
        .satisfies(
            c ->
                assertThat(c.getExpressions())
                    .containsExactly(
                        ConditionBuilder.column(VERSION).isEqualToInt(2),
                        ConditionBuilder.column(ID).isEqualToText(ANY_ID_2),
                        condExpression1,
                        condExpression2));
    assertThat(actual.getTextValue(ID)).isEqualTo(ANY_ID_3);
    assertThat(actual.getIntValue(STATE)).isEqualTo(TransactionState.DELETED.get());
    assertThat(actual.getIntValue(VERSION)).isEqualTo(3);
    assertThat(actual.getBigIntValue(Attribute.PREPARED_AT)).isEqualTo(ANY_TIME_5);
    assertThat(actual.getBigIntValue(Attribute.BEFORE_PREPARED_AT)).isEqualTo(ANY_TIME_3);
    assertThat(actual.getBigIntValue(Attribute.BEFORE_COMMITTED_AT)).isEqualTo(ANY_TIME_4);
    assertThat(actual.getTextValue(Attribute.BEFORE_ID)).isEqualTo(ANY_ID_2);
    assertThat(actual.getIntValue(Attribute.BEFORE_STATE))
        .isEqualTo(TransactionState.COMMITTED.get());
    assertThat(actual.getIntValue(Attribute.BEFORE_VERSION)).isEqualTo(2);
    assertThat(actual.getIntValue(Attribute.BEFORE_PREFIX + ANY_NAME_3)).isEqualTo(ANY_INT_2);
  }

  @Test
  public void
      add_DeleteWithDeleteIfExistsConditionAndResultGiven_ShouldComposePutWithPutIfCondition()
          throws ExecutionException {
    // Arrange
    DeleteIfExists deleteIfExists = mock(DeleteIfExists.class);
    Delete delete = prepareDelete().condition(deleteIfExists).build();
    TransactionResult result = prepareResult();

    // Act
    composer.add(delete, result);

    // Assert
    Put actual = (Put) composer.get().get(0);
    assertThat(actual.forNamespace()).get().isEqualTo(ANY_NAMESPACE_NAME);
    assertThat(actual.forTable()).get().isEqualTo(ANY_TABLE_NAME);
    assertThat(actual.getPartitionKey().equals(Key.ofText(ANY_NAME_1, ANY_TEXT_1))).isTrue();
    assertThat(actual.getClusteringKey()).get().isEqualTo(Key.ofText(ANY_NAME_2, ANY_TEXT_2));
    assertThat(actual.getConsistency()).isEqualTo(Consistency.LINEARIZABLE);
    assertThat(actual.getCondition())
        .get()
        .isInstanceOf(PutIf.class)
        .satisfies(
            c ->
                assertThat(c.getExpressions())
                    .containsExactly(
                        ConditionBuilder.column(VERSION).isEqualToInt(2),
                        ConditionBuilder.column(ID).isEqualToText(ANY_ID_2)));
    assertThat(actual.getTextValue(ID)).isEqualTo(ANY_ID_3);
    assertThat(actual.getIntValue(STATE)).isEqualTo(TransactionState.DELETED.get());
    assertThat(actual.getIntValue(VERSION)).isEqualTo(3);
    assertThat(actual.getBigIntValue(Attribute.PREPARED_AT)).isEqualTo(ANY_TIME_5);
    assertThat(actual.getBigIntValue(Attribute.BEFORE_PREPARED_AT)).isEqualTo(ANY_TIME_3);
    assertThat(actual.getBigIntValue(Attribute.BEFORE_COMMITTED_AT)).isEqualTo(ANY_TIME_4);
    assertThat(actual.getTextValue(Attribute.BEFORE_ID)).isEqualTo(ANY_ID_2);
    assertThat(actual.getIntValue(Attribute.BEFORE_STATE))
        .isEqualTo(TransactionState.COMMITTED.get());
    assertThat(actual.getIntValue(Attribute.BEFORE_VERSION)).isEqualTo(2);
    assertThat(actual.getIntValue(Attribute.BEFORE_PREFIX + ANY_NAME_3)).isEqualTo(ANY_INT_2);
  }

  @Test
  public void add_DeleteWithDeleteIfConditionAndNullResultGiven_ShouldThrowNoMutationException() {
    // Arrange
    Delete delete = prepareDelete().condition(mock(DeleteIf.class)).build();

    // Act
    Assertions.assertThatThrownBy(() -> composer.add(delete, null))
        .isInstanceOf(NoMutationException.class);
  }

  @Test
  public void
      add_DeleteWithDeleteIfExistsConditionAndNullResultGiven_ShouldThrowNoMutationException() {
    // Arrange
    Delete delete = prepareDelete().condition(mock(DeleteIfExists.class)).build();

    // Act
    Assertions.assertThatThrownBy(() -> composer.add(delete, null))
        .isInstanceOf(NoMutationException.class);
  }

  @Test
  public void
      add_GetAndNullResultGiven_ShouldComposePutForPuttingNonExistingRecordForSerializableWithExtraWrite()
          throws ExecutionException {
    // Arrange
    Get get = prepareGet();

    // Act
    composer.add(get, null);

    // Assert
    Put actual = (Put) composer.get().get(0);
    Put expected =
        new Put(get.getPartitionKey(), get.getClusteringKey().orElse(null))
            .forNamespace(get.forNamespace().get())
            .forTable(get.forTable().get());
    expected.withConsistency(Consistency.LINEARIZABLE);
    expected.withCondition(new PutIfNotExists());
    expected.withValue(Attribute.toPreparedAtValue(ANY_TIME_5));
    expected.withValue(Attribute.toIdValue(ANY_ID_3));
    expected.withValue(Attribute.toStateValue(TransactionState.DELETED));
    expected.withValue(Attribute.toVersionValue(1));
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void add_SelectionOtherThanGetGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Scan scan = prepareScan();

    // Act Assert
    assertThatThrownBy(() -> composer.add(scan, null)).isInstanceOf(IllegalArgumentException.class);
  }
}
