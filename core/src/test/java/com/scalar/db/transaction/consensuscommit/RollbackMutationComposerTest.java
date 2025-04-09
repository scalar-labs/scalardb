package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.api.ConditionalExpression.Operator;
import static com.scalar.db.transaction.consensuscommit.Attribute.ID;
import static com.scalar.db.transaction.consensuscommit.Attribute.STATE;
import static com.scalar.db.transaction.consensuscommit.Attribute.toIdValue;
import static com.scalar.db.transaction.consensuscommit.Attribute.toStateValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.Value;
import com.scalar.db.util.ScalarDbUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class RollbackMutationComposerTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_ID_1 = "id1";
  private static final String ANY_ID_2 = "id2";
  private static final long ANY_TIME_1 = 100;
  private static final long ANY_TIME_2 = 200;
  private static final long ANY_TIME_3 = 300;
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
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
              .addPartitionKey(ANY_NAME_1)
              .addClusteringKey(ANY_NAME_2)
              .build());

  private RollbackMutationComposer composer;
  @Mock private DistributedStorage storage;
  @Mock private TransactionTableMetadataManager tableMetadataManager;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    when(tableMetadataManager.getTransactionTableMetadata(any()))
        .thenReturn(new TransactionTableMetadata(TABLE_METADATA));
    composer = new RollbackMutationComposer(ANY_ID_2, storage, tableMetadataManager);
  }

  private Get prepareGet() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Get(partitionKey, clusteringKey)
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private Scan prepareScan() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    return new Scan(partitionKey)
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private Put preparePut() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Put(partitionKey, clusteringKey)
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME)
        .withValue(ANY_NAME_3, ANY_INT_3);
  }

  private TransactionResult prepareResult(TransactionState state) {
    ImmutableMap<String, Column<?>> columns =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1))
            .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_2))
            .put(ANY_NAME_3, IntColumn.of(ANY_NAME_3, ANY_INT_2))
            .put(Attribute.ID, ScalarDbUtils.toColumn(Attribute.toIdValue(ANY_ID_2)))
            .put(
                Attribute.PREPARED_AT,
                ScalarDbUtils.toColumn(Attribute.toPreparedAtValue(ANY_TIME_3)))
            .put(Attribute.STATE, ScalarDbUtils.toColumn(Attribute.toStateValue(state)))
            .put(Attribute.VERSION, ScalarDbUtils.toColumn(Attribute.toVersionValue(2)))
            .put(
                Attribute.BEFORE_PREFIX + ANY_NAME_3,
                IntColumn.of(Attribute.BEFORE_PREFIX + ANY_NAME_3, ANY_INT_1))
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

  private TransactionResult prepareInitialResult(String id, TransactionState state) {
    ImmutableMap.Builder<String, Column<?>> builder =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1))
            .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_2))
            .put(ANY_NAME_3, IntColumn.of(ANY_NAME_3, ANY_INT_1))
            .put(Attribute.ID, ScalarDbUtils.toColumn(Attribute.toIdValue(id)))
            .put(
                Attribute.PREPARED_AT,
                ScalarDbUtils.toColumn(Attribute.toPreparedAtValue(ANY_TIME_1)))
            .put(Attribute.STATE, ScalarDbUtils.toColumn(Attribute.toStateValue(state)))
            .put(Attribute.VERSION, ScalarDbUtils.toColumn(Attribute.toVersionValue(1)))
            .put(Attribute.BEFORE_ID, ScalarDbUtils.toColumn(Attribute.toBeforeIdValue(null)))
            .put(Attribute.BEFORE_VERSION, IntColumn.ofNull(Attribute.BEFORE_VERSION));
    if (state.equals(TransactionState.COMMITTED)) {
      builder.put(
          Attribute.COMMITTED_AT, ScalarDbUtils.toColumn(Attribute.toCommittedAtValue(ANY_TIME_2)));
    }
    return new TransactionResult(new ResultImpl(builder.build(), TABLE_METADATA));
  }

  private TransactionResult prepareResultWithNullMetadata(TransactionState state) {
    ImmutableMap<String, Column<?>> columns =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1))
            .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_2))
            .put(ANY_NAME_3, IntColumn.of(ANY_NAME_3, ANY_INT_2))
            .put(Attribute.ID, TextColumn.of(Attribute.ID, ANY_ID_2))
            .put(Attribute.PREPARED_AT, BigIntColumn.of(Attribute.PREPARED_AT, ANY_TIME_1))
            .put(Attribute.COMMITTED_AT, BigIntColumn.of(Attribute.COMMITTED_AT, ANY_TIME_1))
            .put(Attribute.STATE, IntColumn.of(Attribute.STATE, state.get()))
            .put(Attribute.VERSION, IntColumn.of(Attribute.VERSION, 1))
            .put(
                Attribute.BEFORE_PREFIX + ANY_NAME_3,
                IntColumn.of(Attribute.BEFORE_PREFIX + ANY_NAME_3, ANY_INT_1))
            .put(Attribute.BEFORE_ID, TextColumn.ofNull(Attribute.BEFORE_ID))
            .put(Attribute.BEFORE_PREPARED_AT, BigIntColumn.ofNull(Attribute.BEFORE_PREPARED_AT))
            .put(Attribute.BEFORE_COMMITTED_AT, BigIntColumn.ofNull(Attribute.BEFORE_COMMITTED_AT))
            .put(Attribute.BEFORE_STATE, IntColumn.ofNull(Attribute.BEFORE_STATE))
            .put(Attribute.BEFORE_VERSION, IntColumn.of(Attribute.BEFORE_VERSION, 0))
            .build();
    return new TransactionResult(new ResultImpl(columns, TABLE_METADATA));
  }

  private TransactionResult prepareInitialResultWithNullMetadata() {
    ImmutableMap<String, Column<?>> columns =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1))
            .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_2))
            .put(ANY_NAME_3, IntColumn.of(ANY_NAME_3, ANY_INT_1))
            .put(Attribute.ID, TextColumn.ofNull(Attribute.ID))
            .put(Attribute.PREPARED_AT, BigIntColumn.ofNull(Attribute.PREPARED_AT))
            .put(Attribute.COMMITTED_AT, BigIntColumn.ofNull(Attribute.COMMITTED_AT))
            .put(Attribute.STATE, IntColumn.ofNull(Attribute.STATE))
            .put(Attribute.VERSION, IntColumn.ofNull(Attribute.VERSION))
            .put(Attribute.BEFORE_ID, TextColumn.ofNull(Attribute.BEFORE_ID))
            .put(Attribute.BEFORE_PREPARED_AT, BigIntColumn.ofNull(Attribute.BEFORE_PREPARED_AT))
            .put(Attribute.BEFORE_COMMITTED_AT, BigIntColumn.ofNull(Attribute.BEFORE_COMMITTED_AT))
            .put(Attribute.BEFORE_STATE, IntColumn.ofNull(Attribute.BEFORE_STATE))
            .put(Attribute.BEFORE_VERSION, IntColumn.ofNull(Attribute.BEFORE_VERSION))
            .build();
    return new TransactionResult(new ResultImpl(columns, TABLE_METADATA));
  }

  private List<Value<?>> extractAfterValues(TransactionResult result) {
    List<Value<?>> values = new ArrayList<>();
    result
        .getValues()
        .forEach(
            (k, v) -> {
              if (ConsensusCommitUtils.isAfterImageColumn(k, TABLE_METADATA)
                  && !TABLE_METADATA.getPartitionKeyNames().contains(k)
                  && !TABLE_METADATA.getClusteringKeyNames().contains(k)) {
                values.add(v);
              }
            });
    return values;
  }

  private List<Column<?>> extractAfterColumns(TransactionResult result) {
    List<Column<?>> columns = new ArrayList<>();
    result
        .getColumns()
        .forEach(
            (k, v) -> {
              if (ConsensusCommitUtils.isAfterImageColumn(k, TABLE_METADATA)
                  && !TABLE_METADATA.getPartitionKeyNames().contains(k)
                  && !TABLE_METADATA.getClusteringKeyNames().contains(k)) {
                columns.add(v);
              }
            });
    return columns;
  }

  @Test
  public void add_GetAndPreparedResultByThisGiven_ShouldComposePut() throws ExecutionException {
    // Arrange
    TransactionResult result = prepareResult(TransactionState.PREPARED);
    when(storage.get(any(Get.class))).thenReturn(Optional.of(result));
    Get get = prepareGet();

    // Act
    composer.add(get, result);

    // Assert
    Put actual = (Put) composer.get().get(0);
    Put expected =
        new Put(get.getPartitionKey(), get.getClusteringKey().orElse(null))
            .forNamespace(get.forNamespace().get())
            .forTable(get.forTable().get());
    expected.withConsistency(Consistency.LINEARIZABLE);
    expected.withCondition(
        new PutIf(
            new ConditionalExpression(ID, toIdValue(ANY_ID_2), Operator.EQ),
            new ConditionalExpression(
                STATE, toStateValue(TransactionState.PREPARED), Operator.EQ)));
    expected.withValues(
        extractAfterValues(prepareInitialResult(ANY_ID_1, TransactionState.COMMITTED)));
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void add_GetAndDeletedResultByThisGiven_ShouldComposePut() throws ExecutionException {
    // Arrange
    TransactionResult result = prepareResult(TransactionState.DELETED);
    when(storage.get(any(Get.class))).thenReturn(Optional.of(result));
    Get get = prepareGet();

    // Act
    composer.add(get, result);

    // Assert
    Put actual = (Put) composer.get().get(0);
    Put expected =
        new Put(get.getPartitionKey(), get.getClusteringKey().orElse(null))
            .forNamespace(get.forNamespace().get())
            .forTable(get.forTable().get());
    expected.withConsistency(Consistency.LINEARIZABLE);
    expected.withCondition(
        new PutIf(
            new ConditionalExpression(ID, toIdValue(ANY_ID_2), Operator.EQ),
            new ConditionalExpression(STATE, toStateValue(TransactionState.DELETED), Operator.EQ)));
    expected.withValues(
        extractAfterValues(prepareInitialResult(ANY_ID_1, TransactionState.COMMITTED)));
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void add_PutAndNullResultGivenAndOldResultGivenFromStorage_ShouldDoNothing()
      throws ExecutionException {
    // Arrange
    TransactionResult result = prepareInitialResult(ANY_ID_1, TransactionState.PREPARED);
    when(storage.get(any(Get.class))).thenReturn(Optional.of(result));
    Put put = preparePut();

    // Act
    composer.add(put, null);

    // Assert
    assertThat(composer.get().size()).isEqualTo(0);
    verify(storage).get(any(Get.class));
  }

  @Test
  public void add_PutAndNullResultGivenAndEmptyResultGivenFromStorage_ShouldDoNothing()
      throws ExecutionException {
    // Arrange
    when(storage.get(any(Get.class))).thenReturn(Optional.empty());
    Put put = preparePut();

    // Act
    composer.add(put, null);

    // Assert
    assertThat(composer.get().size()).isEqualTo(0);
    verify(storage).get(any(Get.class));
  }

  @Test
  public void add_PutAndResultFromSnapshotGivenAndPreparedResultGivenFromStorage_ShouldComposePut()
      throws ExecutionException {
    // Arrange
    TransactionResult resultInSnapshot = prepareInitialResult(ANY_ID_1, TransactionState.COMMITTED);
    TransactionResult result = prepareResult(TransactionState.PREPARED);
    when(storage.get(any(Get.class))).thenReturn(Optional.of(result));
    Put put = preparePut();

    // Act
    composer.add(put, resultInSnapshot);

    // Assert
    Put actual = (Put) composer.get().get(0);
    Put expected =
        new Put(put.getPartitionKey(), put.getClusteringKey().orElse(null))
            .forNamespace(put.forNamespace().get())
            .forTable(put.forTable().get());
    expected.withConsistency(Consistency.LINEARIZABLE);
    expected.withCondition(
        new PutIf(
            new ConditionalExpression(ID, toIdValue(ANY_ID_2), Operator.EQ),
            new ConditionalExpression(
                STATE, toStateValue(TransactionState.PREPARED), Operator.EQ)));
    expected.withValues(extractAfterValues(resultInSnapshot));
    assertThat(actual).isEqualTo(expected);
    verify(storage).get(any(Get.class));
  }

  @Test
  public void add_PutAndResultFromSnapshotGivenAndDeletedResultGivenFromStorage_ShouldComposePut()
      throws ExecutionException {
    // Arrange
    TransactionResult resultInSnapshot = prepareInitialResult(ANY_ID_1, TransactionState.COMMITTED);
    TransactionResult result = prepareResult(TransactionState.DELETED);
    when(storage.get(any(Get.class))).thenReturn(Optional.of(result));
    Put put = preparePut();

    // Act
    composer.add(put, resultInSnapshot);

    // Assert
    Put actual = (Put) composer.get().get(0);
    Put expected =
        new Put(put.getPartitionKey(), put.getClusteringKey().orElse(null))
            .forNamespace(put.forNamespace().get())
            .forTable(put.forTable().get());
    expected.withConsistency(Consistency.LINEARIZABLE);
    expected.withCondition(
        new PutIf(
            new ConditionalExpression(ID, toIdValue(ANY_ID_2), Operator.EQ),
            new ConditionalExpression(STATE, toStateValue(TransactionState.DELETED), Operator.EQ)));
    expected.withValues(extractAfterValues(resultInSnapshot));
    assertThat(actual).isEqualTo(expected);
    verify(storage).get(any(Get.class));
  }

  @Test
  public void add_PutAndResultFromSnapshotGivenAndResultFromStorageHasDifferentId_ShouldDoNothing()
      throws ExecutionException {
    // Arrange
    TransactionResult result = prepareInitialResult(ANY_ID_1, TransactionState.COMMITTED);
    when(storage.get(any(Get.class))).thenReturn(Optional.of(result));
    Put put = preparePut();

    // Act
    composer.add(put, result);

    // Assert
    assertThat(composer.get().size()).isEqualTo(0);
    verify(storage).get(any(Get.class));
  }

  @Test
  public void add_PutAndResultFromSnapshotGivenAndItsAlreadyRollbackDeleted_ShouldDoNothing()
      throws ExecutionException {
    // Arrange
    TransactionResult result = prepareInitialResult(ANY_ID_1, TransactionState.COMMITTED);
    when(storage.get(any(Get.class))).thenReturn(Optional.empty());
    Put put = preparePut();

    // Act
    composer.add(put, result);

    // Assert
    assertThat(composer.get().size()).isEqualTo(0);
    verify(storage).get(any(Get.class));
  }

  @Test
  public void add_ScanAndPreparedResultByThisGiven_ShouldComposePut() throws ExecutionException {
    // Arrange
    TransactionResult result = prepareResult(TransactionState.PREPARED);
    when(storage.get(any(Get.class))).thenReturn(Optional.of(result));
    Scan scan = prepareScan();

    // Act
    composer.add(scan, result);

    // Assert
    Put actual = (Put) composer.get().get(0);
    Put expected =
        new Put(scan.getPartitionKey(), result.getClusteringKey().orElse(null))
            .forNamespace(scan.forNamespace().get())
            .forTable(scan.forTable().get());
    expected.withConsistency(Consistency.LINEARIZABLE);
    expected.withCondition(
        new PutIf(
            new ConditionalExpression(ID, toIdValue(ANY_ID_2), Operator.EQ),
            new ConditionalExpression(
                STATE, toStateValue(TransactionState.PREPARED), Operator.EQ)));
    expected.withValues(
        extractAfterValues(prepareInitialResult(ANY_ID_1, TransactionState.COMMITTED)));
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void add_ScanAndDeletedResultByThisGiven_ShouldComposePut() throws ExecutionException {
    // Arrange
    TransactionResult result = prepareResult(TransactionState.DELETED);
    when(storage.get(any(Get.class))).thenReturn(Optional.of(result));
    Scan scan = prepareScan();

    // Act
    composer.add(scan, result);

    // Assert
    Put actual = (Put) composer.get().get(0);
    Put expected =
        new Put(scan.getPartitionKey(), result.getClusteringKey().orElse(null))
            .forNamespace(scan.forNamespace().get())
            .forTable(scan.forTable().get());
    expected.withConsistency(Consistency.LINEARIZABLE);
    expected.withCondition(
        new PutIf(
            new ConditionalExpression(ID, toIdValue(ANY_ID_2), Operator.EQ),
            new ConditionalExpression(STATE, toStateValue(TransactionState.DELETED), Operator.EQ)));
    expected.withValues(
        extractAfterValues(prepareInitialResult(ANY_ID_1, TransactionState.COMMITTED)));
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void add_ScanAndPreparedResultByThisGivenAndBeforeResultNotGiven_ShouldComposeDelete()
      throws ExecutionException {
    // Arrange
    TransactionResult result = prepareInitialResult(ANY_ID_2, TransactionState.PREPARED);
    when(storage.get(any(Get.class))).thenReturn(Optional.of(result));
    Scan scan = prepareScan();

    // Act
    composer.add(scan, result);

    // Assert
    Delete actual = (Delete) composer.get().get(0);
    Delete expected =
        new Delete(scan.getPartitionKey(), result.getClusteringKey().orElse(null))
            .forNamespace(scan.forNamespace().get())
            .forTable(scan.forTable().get());
    expected.withConsistency(Consistency.LINEARIZABLE);
    expected.withCondition(
        new DeleteIf(
            new ConditionalExpression(ID, toIdValue(ANY_ID_2), Operator.EQ),
            new ConditionalExpression(
                STATE, toStateValue(TransactionState.PREPARED), Operator.EQ)));
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void add_GetAndPreparedResultWithNullMetadataByThisGiven_ShouldComposePut()
      throws ExecutionException {
    // Arrange
    TransactionResult result = prepareResultWithNullMetadata(TransactionState.PREPARED);
    when(storage.get(any(Get.class))).thenReturn(Optional.of(result));
    Get get = prepareGet();

    // Act
    composer.add(get, result);

    // Assert
    Put actual = (Put) composer.get().get(0);
    PutBuilder.Buildable builder =
        Put.newBuilder()
            .namespace(get.forNamespace().get())
            .table(get.forTable().get())
            .partitionKey(get.getPartitionKey())
            .clusteringKey(get.getClusteringKey().get())
            .consistency(Consistency.LINEARIZABLE)
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column(ID).isEqualToText(ANY_ID_2))
                    .and(
                        ConditionBuilder.column(STATE)
                            .isEqualToInt(TransactionState.PREPARED.get()))
                    .build());
    extractAfterColumns(prepareInitialResultWithNullMetadata()).forEach(builder::value);
    Put expected = builder.build();
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void add_GetAndInitialResultWithNullMetadataGivenFromStorage_ShouldDoNothing()
      throws ExecutionException {
    // Arrange
    TransactionResult obtainedResult = prepareResultWithNullMetadata(TransactionState.PREPARED);
    TransactionResult currentResult = prepareInitialResultWithNullMetadata();
    when(storage.get(any(Get.class))).thenReturn(Optional.of(currentResult));
    Get get = prepareGet();

    // Act
    composer.add(get, obtainedResult);

    // Assert
    assertThat(composer.get().size()).isEqualTo(0);
    verify(storage).get(any(Get.class));
  }
}
