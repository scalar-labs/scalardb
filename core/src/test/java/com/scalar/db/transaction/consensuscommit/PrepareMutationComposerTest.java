package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.transaction.consensuscommit.Attribute.ID;
import static com.scalar.db.transaction.consensuscommit.Attribute.VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.util.ScalarDbUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

  private PrepareMutationComposer composer;

  @BeforeEach
  public void setUp() {
    composer = new PrepareMutationComposer(ANY_ID_3, ANY_TIME_5);
  }

  private Put preparePut() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Put(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME)
        .withValue(ANY_NAME_3, ANY_INT_3);
  }

  private Delete prepareDelete() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Delete(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
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

  private TransactionResult prepareResultWithNullMetadata() {
    ImmutableMap<String, Column<?>> columns =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1))
            .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_2))
            .put(ANY_NAME_3, IntColumn.of(ANY_NAME_3, ANY_INT_2))
            .put(Attribute.ID, TextColumn.ofNull(Attribute.ID))
            .put(Attribute.PREPARED_AT, BigIntColumn.ofNull(Attribute.PREPARED_AT))
            .put(Attribute.COMMITTED_AT, BigIntColumn.ofNull(Attribute.COMMITTED_AT))
            .put(Attribute.STATE, IntColumn.ofNull(Attribute.STATE))
            .put(Attribute.VERSION, IntColumn.ofNull(Attribute.VERSION))
            .put(
                Attribute.BEFORE_PREFIX + ANY_NAME_3,
                IntColumn.of(Attribute.BEFORE_PREFIX + ANY_NAME_3, ANY_INT_1))
            .put(Attribute.BEFORE_ID, TextColumn.ofNull(Attribute.BEFORE_ID))
            .put(Attribute.BEFORE_PREPARED_AT, BigIntColumn.ofNull(Attribute.BEFORE_PREPARED_AT))
            .put(Attribute.BEFORE_COMMITTED_AT, BigIntColumn.ofNull(Attribute.BEFORE_COMMITTED_AT))
            .put(Attribute.BEFORE_STATE, IntColumn.ofNull(Attribute.BEFORE_STATE))
            .put(Attribute.BEFORE_VERSION, IntColumn.ofNull(Attribute.BEFORE_VERSION))
            .build();
    return new TransactionResult(new ResultImpl(columns, TABLE_METADATA));
  }

  @Test
  public void add_PutAndResultGiven_ShouldComposePutWithPutIfCondition() {
    // Arrange
    Put put = preparePut();
    TransactionResult result = prepareResult();

    // Act
    composer.add(put, result);

    // Assert
    Put actual = (Put) composer.get().get(0);
    Put expected =
        Put.newBuilder(put)
            .consistency(Consistency.LINEARIZABLE)
            .textValue(Attribute.ID, ANY_ID_3)
            .intValue(Attribute.STATE, TransactionState.PREPARED.get())
            .intValue(Attribute.VERSION, 3)
            .bigIntValue(Attribute.PREPARED_AT, ANY_TIME_5)
            .bigIntValue(Attribute.BEFORE_PREPARED_AT, ANY_TIME_3)
            .bigIntValue(Attribute.BEFORE_COMMITTED_AT, ANY_TIME_4)
            .textValue(Attribute.BEFORE_ID, ANY_ID_2)
            .intValue(Attribute.BEFORE_STATE, TransactionState.COMMITTED.get())
            .intValue(Attribute.BEFORE_VERSION, 2)
            .intValue(Attribute.BEFORE_PREFIX + ANY_NAME_3, ANY_INT_2)
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column(ID).isEqualToText(ANY_ID_2))
                    .and(ConditionBuilder.column(VERSION).isEqualToInt(2))
                    .build())
            .build();
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void
      add_PutAndResultWithNullTxIdGiven_ShouldComposePutWithBeforeVersion0AndPutIfNullCondition() {
    // Arrange
    Put put = preparePut();
    TransactionResult result = prepareResultWithNullMetadata();

    // Act
    composer.add(put, result);

    // Assert
    Put actual = (Put) composer.get().get(0);
    Put expected =
        Put.newBuilder(put)
            .consistency(Consistency.LINEARIZABLE)
            .textValue(Attribute.ID, ANY_ID_3)
            .intValue(Attribute.STATE, TransactionState.PREPARED.get())
            .intValue(Attribute.VERSION, 1)
            .bigIntValue(Attribute.PREPARED_AT, ANY_TIME_5)
            .bigIntValue(Attribute.BEFORE_PREPARED_AT, null)
            .bigIntValue(Attribute.BEFORE_COMMITTED_AT, null)
            .textValue(Attribute.BEFORE_ID, null)
            .intValue(Attribute.BEFORE_STATE, null)
            .intValue(Attribute.BEFORE_VERSION, 0)
            .intValue(Attribute.BEFORE_PREFIX + ANY_NAME_3, ANY_INT_2)
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column(ID).isNullText())
                    .and(ConditionBuilder.column(VERSION).isNullInt())
                    .build())
            .build();
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void add_PutAndNullResultGiven_ShouldComposePutWithPutIfNotExistsCondition() {
    // Arrange
    Put put = preparePut();

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
  public void add_DeleteAndResultGiven_ShouldComposePutWithPutIfCondition() {
    // Arrange
    Delete delete = prepareDelete();
    TransactionResult result = prepareResult();

    // Act
    composer.add(delete, result);

    // Assert
    Put actual = (Put) composer.get().get(0);
    Put expected =
        Put.newBuilder()
            .namespace(delete.forNamespace().get())
            .table(delete.forTable().get())
            .partitionKey(delete.getPartitionKey())
            .clusteringKey(delete.getClusteringKey().orElse(null))
            .consistency(Consistency.LINEARIZABLE)
            .textValue(Attribute.ID, ANY_ID_3)
            .intValue(Attribute.STATE, TransactionState.DELETED.get())
            .intValue(Attribute.VERSION, 3)
            .bigIntValue(Attribute.PREPARED_AT, ANY_TIME_5)
            .bigIntValue(Attribute.BEFORE_PREPARED_AT, ANY_TIME_3)
            .bigIntValue(Attribute.BEFORE_COMMITTED_AT, ANY_TIME_4)
            .textValue(Attribute.BEFORE_ID, ANY_ID_2)
            .intValue(Attribute.BEFORE_STATE, TransactionState.COMMITTED.get())
            .intValue(Attribute.BEFORE_VERSION, 2)
            .intValue(Attribute.BEFORE_PREFIX + ANY_NAME_3, ANY_INT_2)
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column(ID).isEqualToText(ANY_ID_2))
                    .and(ConditionBuilder.column(VERSION).isEqualToInt(2))
                    .build())
            .build();
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void
      add_DeleteAndResultWithNullTxIdGiven_ShouldComposePutWithBeforeVersion0AndPutIfNullCondition() {
    // Arrange
    Delete delete = prepareDelete();
    TransactionResult result = prepareResultWithNullMetadata();

    // Act
    composer.add(delete, result);

    // Assert
    Put actual = (Put) composer.get().get(0);
    Put expected =
        Put.newBuilder()
            .namespace(delete.forNamespace().get())
            .table(delete.forTable().get())
            .partitionKey(delete.getPartitionKey())
            .clusteringKey(delete.getClusteringKey().orElse(null))
            .consistency(Consistency.LINEARIZABLE)
            .textValue(Attribute.ID, ANY_ID_3)
            .intValue(Attribute.STATE, TransactionState.DELETED.get())
            .intValue(Attribute.VERSION, 1)
            .bigIntValue(Attribute.PREPARED_AT, ANY_TIME_5)
            .bigIntValue(Attribute.BEFORE_PREPARED_AT, null)
            .bigIntValue(Attribute.BEFORE_COMMITTED_AT, null)
            .textValue(Attribute.BEFORE_ID, null)
            .intValue(Attribute.BEFORE_STATE, null)
            .intValue(Attribute.BEFORE_VERSION, 0)
            .intValue(Attribute.BEFORE_PREFIX + ANY_NAME_3, ANY_INT_2)
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column(ID).isNullText())
                    .and(ConditionBuilder.column(VERSION).isNullInt())
                    .build())
            .build();
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void add_DeleteAndNullResultGiven_ShouldComposePutWithPutIfNotExistsCondition() {
    // Arrange
    Delete delete = prepareDelete();

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
  public void
      add_GetAndNullResultGiven_ShouldComposePutForPuttingNonExistingRecordForSerializableWithExtraWrite() {
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
