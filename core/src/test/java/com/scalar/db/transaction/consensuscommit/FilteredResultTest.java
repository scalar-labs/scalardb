package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.TextValue;
import com.scalar.db.util.ScalarDbUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FilteredResultTest {

  private static final String ACCOUNT_ID = "account_id";
  private static final String ACCOUNT_TYPE = "account_type";
  private static final String BALANCE = "balance";

  private static final TableMetadata TABLE_METADATA =
      ConsensusCommitUtils.buildTransactionTableMetadata(
          TableMetadata.newBuilder()
              .addColumn(ACCOUNT_ID, DataType.INT)
              .addColumn(ACCOUNT_TYPE, DataType.INT)
              .addColumn(BALANCE, DataType.INT)
              .addPartitionKey(ACCOUNT_ID)
              .addClusteringKey(ACCOUNT_TYPE)
              .build());

  private static final IntValue ACCOUNT_ID_VALUE = new IntValue(ACCOUNT_ID, 0);
  private static final IntValue ACCOUNT_TYPE_VALUE = new IntValue(ACCOUNT_TYPE, 1);
  private static final IntValue BALANCE_VALUE = new IntValue(BALANCE, 2);
  private static final TextValue ID_VALUE = Attribute.toIdValue("aaa");
  private static final IntValue STATE_VALUE = Attribute.toStateValue(TransactionState.COMMITTED);
  private static final IntValue VERSION_VALUE = Attribute.toVersionValue(4);
  private static final BigIntValue PREPARED_AT_VALUE = Attribute.toPreparedAtValue(5);
  private static final BigIntValue COMMITTED_AT_VALUE = Attribute.toCommittedAtValue(6);
  private static final IntValue BEFORE_BALANCE_VALUE =
      new IntValue(Attribute.BEFORE_PREFIX + BALANCE, 7);
  private static final TextValue BEFORE_ID_VALUE = Attribute.toBeforeIdValue("bbb");
  private static final IntValue BEFORE_STATE_VALUE =
      Attribute.toBeforeStateValue(TransactionState.COMMITTED);
  private static final IntValue BEFORE_VERSION_VALUE = Attribute.toBeforeVersionValue(8);
  private static final BigIntValue BEFORE_PREPARED_AT_VALUE = Attribute.toBeforePreparedAtValue(9);
  private static final BigIntValue BEFORE_COMMITTED_AT_VALUE =
      Attribute.toBeforeCommittedAtValue(11);

  private Result result;

  @BeforeEach
  public void setUp() {
    // Arrange
    Map<String, Column<?>> columns =
        ImmutableMap.<String, Column<?>>builder()
            .put(ACCOUNT_ID, ScalarDbUtils.toColumn(ACCOUNT_ID_VALUE))
            .put(ACCOUNT_TYPE, ScalarDbUtils.toColumn(ACCOUNT_TYPE_VALUE))
            .put(BALANCE, ScalarDbUtils.toColumn(BALANCE_VALUE))
            .put(Attribute.ID, ScalarDbUtils.toColumn(ID_VALUE))
            .put(Attribute.STATE, ScalarDbUtils.toColumn(STATE_VALUE))
            .put(Attribute.VERSION, ScalarDbUtils.toColumn(VERSION_VALUE))
            .put(Attribute.PREPARED_AT, ScalarDbUtils.toColumn(PREPARED_AT_VALUE))
            .put(Attribute.COMMITTED_AT, ScalarDbUtils.toColumn(COMMITTED_AT_VALUE))
            .put(Attribute.BEFORE_PREFIX + BALANCE, ScalarDbUtils.toColumn(BEFORE_BALANCE_VALUE))
            .put(Attribute.BEFORE_ID, ScalarDbUtils.toColumn(BEFORE_ID_VALUE))
            .put(Attribute.BEFORE_STATE, ScalarDbUtils.toColumn(BEFORE_STATE_VALUE))
            .put(Attribute.BEFORE_VERSION, ScalarDbUtils.toColumn(BEFORE_VERSION_VALUE))
            .put(Attribute.BEFORE_PREPARED_AT, ScalarDbUtils.toColumn(BEFORE_PREPARED_AT_VALUE))
            .put(Attribute.BEFORE_COMMITTED_AT, ScalarDbUtils.toColumn(BEFORE_COMMITTED_AT_VALUE))
            .build();

    result = new ResultImpl(columns, TABLE_METADATA);
  }

  @Test
  public void withoutProjections_ShouldFilterOutTransactionMetaColumns() {
    // Arrange
    FilteredResult filteredResult =
        new FilteredResult(result, Collections.emptyList(), TABLE_METADATA, false);

    // Act Assert
    assertThat(filteredResult.getPartitionKey()).isPresent();
    assertThat(filteredResult.getPartitionKey().get().size()).isEqualTo(1);
    assertThat(filteredResult.getPartitionKey().get().get().get(0)).isEqualTo(ACCOUNT_ID_VALUE);

    assertThat(filteredResult.getClusteringKey()).isPresent();
    assertThat(filteredResult.getClusteringKey().get().size()).isEqualTo(1);
    assertThat(filteredResult.getClusteringKey().get().get().get(0)).isEqualTo(ACCOUNT_TYPE_VALUE);

    assertThat(filteredResult.getValue(ACCOUNT_ID).isPresent()).isTrue();
    assertThat(filteredResult.getValue(ACCOUNT_ID).get()).isEqualTo(ACCOUNT_ID_VALUE);
    assertThat(filteredResult.getValue(ACCOUNT_TYPE).isPresent()).isTrue();
    assertThat(filteredResult.getValue(ACCOUNT_TYPE).get()).isEqualTo(ACCOUNT_TYPE_VALUE);
    assertThat(filteredResult.getValue(BALANCE).isPresent()).isTrue();
    assertThat(filteredResult.getValue(BALANCE).get()).isEqualTo(BALANCE_VALUE);

    assertThat(filteredResult.getValue(Attribute.ID)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.STATE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.VERSION)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.PREPARED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.COMMITTED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_PREFIX + BALANCE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_ID)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_STATE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_VERSION)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_PREPARED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_COMMITTED_AT)).isNotPresent();

    assertThat(filteredResult.getContainedColumnNames())
        .isEqualTo(new HashSet<>(Arrays.asList(ACCOUNT_ID, ACCOUNT_TYPE, BALANCE)));

    assertThat(filteredResult.contains(ACCOUNT_ID)).isTrue();
    assertThat(filteredResult.isNull(ACCOUNT_ID)).isFalse();
    assertThat(filteredResult.getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(filteredResult.getAsObject(ACCOUNT_ID)).isEqualTo(0);

    assertThat(filteredResult.contains(ACCOUNT_TYPE)).isTrue();
    assertThat(filteredResult.isNull(ACCOUNT_TYPE)).isFalse();
    assertThat(filteredResult.getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(filteredResult.getAsObject(ACCOUNT_TYPE)).isEqualTo(1);

    assertThat(filteredResult.contains(BALANCE)).isTrue();
    assertThat(filteredResult.isNull(BALANCE)).isFalse();
    assertThat(filteredResult.getInt(BALANCE)).isEqualTo(2);
    assertThat(filteredResult.getAsObject(BALANCE)).isEqualTo(2);

    assertThat(filteredResult.contains(Attribute.ID)).isFalse();
    assertThat(filteredResult.contains(Attribute.STATE)).isFalse();
    assertThat(filteredResult.contains(Attribute.VERSION)).isFalse();
    assertThat(filteredResult.contains(Attribute.PREPARED_AT)).isFalse();
    assertThat(filteredResult.contains(Attribute.COMMITTED_AT)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_PREFIX + BALANCE)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_ID)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_STATE)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_VERSION)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_PREPARED_AT)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_COMMITTED_AT)).isFalse();
  }

  @Test
  public void withoutProjectionsAndIncludeMetadataEnabled_ShouldContainAllColumns() {
    // Arrange
    FilteredResult filteredResult =
        new FilteredResult(result, Collections.emptyList(), TABLE_METADATA, true);

    // Act Assert
    assertThat(filteredResult.getPartitionKey()).isPresent();
    assertThat(filteredResult.getPartitionKey().get().size()).isEqualTo(1);
    assertThat(filteredResult.getPartitionKey().get().get().get(0)).isEqualTo(ACCOUNT_ID_VALUE);

    assertThat(filteredResult.getClusteringKey()).isPresent();
    assertThat(filteredResult.getClusteringKey().get().size()).isEqualTo(1);
    assertThat(filteredResult.getClusteringKey().get().get().get(0)).isEqualTo(ACCOUNT_TYPE_VALUE);
    assertThat(filteredResult.getValue(ACCOUNT_ID).isPresent()).isTrue();
    assertThat(filteredResult.getValue(ACCOUNT_ID).get()).isEqualTo(ACCOUNT_ID_VALUE);
    assertThat(filteredResult.getValue(ACCOUNT_TYPE).isPresent()).isTrue();
    assertThat(filteredResult.getValue(ACCOUNT_TYPE).get()).isEqualTo(ACCOUNT_TYPE_VALUE);
    assertThat(filteredResult.getValue(BALANCE).isPresent()).isTrue();
    assertThat(filteredResult.getValue(BALANCE).get()).isEqualTo(BALANCE_VALUE);

    assertThat(filteredResult.getValue(Attribute.ID)).isPresent();
    assertThat(filteredResult.getValue(Attribute.ID).get()).isEqualTo(ID_VALUE);
    assertThat(filteredResult.getValue(Attribute.STATE)).isPresent();
    assertThat(filteredResult.getValue(Attribute.STATE).get()).isEqualTo(STATE_VALUE);
    assertThat(filteredResult.getValue(Attribute.VERSION)).isPresent();
    assertThat(filteredResult.getValue(Attribute.VERSION).get()).isEqualTo(VERSION_VALUE);
    assertThat(filteredResult.getValue(Attribute.PREPARED_AT)).isPresent();
    assertThat(filteredResult.getValue(Attribute.PREPARED_AT).get()).isEqualTo(PREPARED_AT_VALUE);
    assertThat(filteredResult.getValue(Attribute.COMMITTED_AT)).isPresent();
    assertThat(filteredResult.getValue(Attribute.COMMITTED_AT).get()).isEqualTo(COMMITTED_AT_VALUE);
    assertThat(filteredResult.getValue(Attribute.BEFORE_PREFIX + BALANCE)).isPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_PREFIX + BALANCE).get())
        .isEqualTo(BEFORE_BALANCE_VALUE);
    assertThat(filteredResult.getValue(Attribute.BEFORE_ID)).isPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_ID).get()).isEqualTo(BEFORE_ID_VALUE);
    assertThat(filteredResult.getValue(Attribute.BEFORE_STATE)).isPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_STATE).get()).isEqualTo(BEFORE_STATE_VALUE);
    assertThat(filteredResult.getValue(Attribute.BEFORE_VERSION)).isPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_VERSION).get())
        .isEqualTo(BEFORE_VERSION_VALUE);
    assertThat(filteredResult.getValue(Attribute.BEFORE_PREPARED_AT)).isPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_PREPARED_AT).get())
        .isEqualTo(BEFORE_PREPARED_AT_VALUE);
    assertThat(filteredResult.getValue(Attribute.BEFORE_COMMITTED_AT)).isPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_COMMITTED_AT).get())
        .isEqualTo(BEFORE_COMMITTED_AT_VALUE);

    assertThat(filteredResult.getContainedColumnNames())
        .containsOnly(
            ACCOUNT_ID,
            ACCOUNT_TYPE,
            BALANCE,
            Attribute.ID,
            Attribute.STATE,
            Attribute.VERSION,
            Attribute.PREPARED_AT,
            Attribute.COMMITTED_AT,
            Attribute.BEFORE_PREFIX + BALANCE,
            Attribute.BEFORE_ID,
            Attribute.BEFORE_STATE,
            Attribute.BEFORE_VERSION,
            Attribute.BEFORE_PREPARED_AT,
            Attribute.BEFORE_COMMITTED_AT);

    assertThat(filteredResult.contains(ACCOUNT_ID)).isTrue();
    assertThat(filteredResult.isNull(ACCOUNT_ID)).isFalse();
    assertThat(filteredResult.getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(filteredResult.getAsObject(ACCOUNT_ID)).isEqualTo(0);

    assertThat(filteredResult.contains(ACCOUNT_TYPE)).isTrue();
    assertThat(filteredResult.isNull(ACCOUNT_TYPE)).isFalse();
    assertThat(filteredResult.getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(filteredResult.getAsObject(ACCOUNT_TYPE)).isEqualTo(1);

    assertThat(filteredResult.contains(BALANCE)).isTrue();
    assertThat(filteredResult.isNull(BALANCE)).isFalse();
    assertThat(filteredResult.getInt(BALANCE)).isEqualTo(2);
    assertThat(filteredResult.getAsObject(BALANCE)).isEqualTo(2);

    assertThat(filteredResult.contains(Attribute.ID)).isTrue();
    assertThat(filteredResult.isNull(Attribute.ID)).isFalse();
    assertThat(filteredResult.getText(Attribute.ID)).isEqualTo("aaa");
    assertThat(filteredResult.getAsObject(Attribute.ID)).isEqualTo("aaa");

    assertThat(filteredResult.contains(Attribute.STATE)).isTrue();
    assertThat(filteredResult.isNull(Attribute.STATE)).isFalse();
    assertThat(filteredResult.getInt(Attribute.STATE)).isEqualTo(TransactionState.COMMITTED.get());
    assertThat(filteredResult.getAsObject(Attribute.STATE))
        .isEqualTo(TransactionState.COMMITTED.get());

    assertThat(filteredResult.contains(Attribute.VERSION)).isTrue();
    assertThat(filteredResult.isNull(Attribute.VERSION)).isFalse();
    assertThat(filteredResult.getInt(Attribute.VERSION)).isEqualTo(4);
    assertThat(filteredResult.getAsObject(Attribute.VERSION)).isEqualTo(4);

    assertThat(filteredResult.contains(Attribute.PREPARED_AT)).isTrue();
    assertThat(filteredResult.isNull(Attribute.PREPARED_AT)).isFalse();
    assertThat(filteredResult.getBigInt(Attribute.PREPARED_AT)).isEqualTo(5);
    assertThat(filteredResult.getAsObject(Attribute.PREPARED_AT)).isEqualTo(5L);

    assertThat(filteredResult.contains(Attribute.COMMITTED_AT)).isTrue();
    assertThat(filteredResult.isNull(Attribute.COMMITTED_AT)).isFalse();
    assertThat(filteredResult.getBigInt(Attribute.COMMITTED_AT)).isEqualTo(6);
    assertThat(filteredResult.getAsObject(Attribute.COMMITTED_AT)).isEqualTo(6L);

    assertThat(filteredResult.contains(Attribute.BEFORE_PREFIX + BALANCE)).isTrue();
    assertThat(filteredResult.isNull(Attribute.BEFORE_PREFIX + BALANCE)).isFalse();
    assertThat(filteredResult.getInt(Attribute.BEFORE_PREFIX + BALANCE)).isEqualTo(7);
    assertThat(filteredResult.getAsObject(Attribute.BEFORE_PREFIX + BALANCE)).isEqualTo(7);

    assertThat(filteredResult.contains(Attribute.BEFORE_ID)).isTrue();
    assertThat(filteredResult.isNull(Attribute.BEFORE_ID)).isFalse();
    assertThat(filteredResult.getText(Attribute.BEFORE_ID)).isEqualTo("bbb");
    assertThat(filteredResult.getAsObject(Attribute.BEFORE_ID)).isEqualTo("bbb");

    assertThat(filteredResult.contains(Attribute.BEFORE_STATE)).isTrue();
    assertThat(filteredResult.isNull(Attribute.BEFORE_STATE)).isFalse();
    assertThat(filteredResult.getInt(Attribute.BEFORE_STATE))
        .isEqualTo(TransactionState.COMMITTED.get());
    assertThat(filteredResult.getAsObject(Attribute.BEFORE_STATE))
        .isEqualTo(TransactionState.COMMITTED.get());

    assertThat(filteredResult.contains(Attribute.BEFORE_VERSION)).isTrue();
    assertThat(filteredResult.isNull(Attribute.BEFORE_VERSION)).isFalse();
    assertThat(filteredResult.getInt(Attribute.BEFORE_VERSION)).isEqualTo(8);
    assertThat(filteredResult.getAsObject(Attribute.BEFORE_VERSION)).isEqualTo(8);

    assertThat(filteredResult.contains(Attribute.BEFORE_PREPARED_AT)).isTrue();
    assertThat(filteredResult.isNull(Attribute.BEFORE_PREPARED_AT)).isFalse();
    assertThat(filteredResult.getBigInt(Attribute.BEFORE_PREPARED_AT)).isEqualTo(9);
    assertThat(filteredResult.getAsObject(Attribute.BEFORE_PREPARED_AT)).isEqualTo(9L);

    assertThat(filteredResult.contains(Attribute.BEFORE_COMMITTED_AT)).isTrue();
    assertThat(filteredResult.isNull(Attribute.BEFORE_COMMITTED_AT)).isFalse();
    assertThat(filteredResult.getBigInt(Attribute.BEFORE_COMMITTED_AT)).isEqualTo(11);
    assertThat(filteredResult.getAsObject(Attribute.BEFORE_COMMITTED_AT)).isEqualTo(11L);
  }

  @Test
  public void withProjections_ShouldFilterOutUnprojectedColumnsAndTransactionMetaColumns() {
    // Arrange
    FilteredResult filteredResult =
        new FilteredResult(result, Arrays.asList(ACCOUNT_ID, BALANCE), TABLE_METADATA, false);

    // Act Assert
    assertThat(filteredResult.getPartitionKey()).isPresent();
    assertThat(filteredResult.getPartitionKey().get().size()).isEqualTo(1);
    assertThat(filteredResult.getPartitionKey().get().get().get(0)).isEqualTo(ACCOUNT_ID_VALUE);

    assertThat(catchThrowable(filteredResult::getClusteringKey))
        .isInstanceOf(IllegalStateException.class);

    assertThat(filteredResult.getValue(ACCOUNT_ID).isPresent()).isTrue();
    assertThat(filteredResult.getValue(ACCOUNT_ID).get()).isEqualTo(ACCOUNT_ID_VALUE);
    assertThat(filteredResult.getValue(ACCOUNT_TYPE)).isNotPresent();
    assertThat(filteredResult.getValue(BALANCE).isPresent()).isTrue();
    assertThat(filteredResult.getValue(BALANCE).get()).isEqualTo(BALANCE_VALUE);

    assertThat(filteredResult.getValue(Attribute.ID)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.STATE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.VERSION)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.PREPARED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.COMMITTED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_PREFIX + BALANCE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_ID)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_STATE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_VERSION)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_PREPARED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_COMMITTED_AT)).isNotPresent();

    assertThat(filteredResult.getContainedColumnNames())
        .isEqualTo(new HashSet<>(Arrays.asList(ACCOUNT_ID, BALANCE)));

    assertThat(filteredResult.contains(ACCOUNT_ID)).isTrue();
    assertThat(filteredResult.isNull(ACCOUNT_ID)).isFalse();
    assertThat(filteredResult.getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(filteredResult.getAsObject(ACCOUNT_ID)).isEqualTo(0);

    assertThat(filteredResult.contains(ACCOUNT_TYPE)).isFalse();

    assertThat(filteredResult.contains(BALANCE)).isTrue();
    assertThat(filteredResult.isNull(BALANCE)).isFalse();
    assertThat(filteredResult.getInt(BALANCE)).isEqualTo(2);
    assertThat(filteredResult.getAsObject(BALANCE)).isEqualTo(2);

    assertThat(filteredResult.contains(Attribute.ID)).isFalse();
    assertThat(filteredResult.contains(Attribute.STATE)).isFalse();
    assertThat(filteredResult.contains(Attribute.VERSION)).isFalse();
    assertThat(filteredResult.contains(Attribute.PREPARED_AT)).isFalse();
    assertThat(filteredResult.contains(Attribute.COMMITTED_AT)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_PREFIX + BALANCE)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_ID)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_STATE)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_VERSION)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_PREPARED_AT)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_COMMITTED_AT)).isFalse();
  }

  @Test
  public void withProjectionsAndIncludeMetadataEnabled_ShouldNotIncludeNonProjectedColumns() {
    // Arrange
    FilteredResult filteredResult =
        new FilteredResult(
            result,
            Arrays.asList(
                ACCOUNT_ID, BALANCE, Attribute.VERSION, Attribute.BEFORE_PREFIX + BALANCE),
            TABLE_METADATA,
            true);

    // Act Assert
    assertThat(filteredResult.getPartitionKey()).isPresent();
    assertThat(filteredResult.getPartitionKey().get().size()).isEqualTo(1);
    assertThat(filteredResult.getPartitionKey().get().get().get(0)).isEqualTo(ACCOUNT_ID_VALUE);

    assertThat(catchThrowable(filteredResult::getClusteringKey))
        .isInstanceOf(IllegalStateException.class);

    assertThat(filteredResult.getValue(ACCOUNT_ID).isPresent()).isTrue();
    assertThat(filteredResult.getValue(ACCOUNT_ID).get()).isEqualTo(ACCOUNT_ID_VALUE);
    assertThat(filteredResult.getValue(ACCOUNT_TYPE)).isNotPresent();
    assertThat(filteredResult.getValue(BALANCE).isPresent()).isTrue();
    assertThat(filteredResult.getValue(BALANCE).get()).isEqualTo(BALANCE_VALUE);

    assertThat(filteredResult.getValue(Attribute.ID)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.STATE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.VERSION)).isPresent();
    assertThat(filteredResult.getValue(Attribute.VERSION).get()).isEqualTo(VERSION_VALUE);
    assertThat(filteredResult.getValue(Attribute.PREPARED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.COMMITTED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_PREFIX + BALANCE)).isPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_PREFIX + BALANCE).get())
        .isEqualTo(BEFORE_BALANCE_VALUE);
    assertThat(filteredResult.getValue(Attribute.BEFORE_ID)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_STATE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_VERSION)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_PREPARED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_COMMITTED_AT)).isNotPresent();

    assertThat(filteredResult.getContainedColumnNames())
        .containsOnly(ACCOUNT_ID, BALANCE, Attribute.VERSION, Attribute.BEFORE_PREFIX + BALANCE);

    assertThat(filteredResult.contains(ACCOUNT_ID)).isTrue();
    assertThat(filteredResult.isNull(ACCOUNT_ID)).isFalse();
    assertThat(filteredResult.getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(filteredResult.getAsObject(ACCOUNT_ID)).isEqualTo(0);

    assertThat(filteredResult.contains(ACCOUNT_TYPE)).isFalse();

    assertThat(filteredResult.contains(BALANCE)).isTrue();
    assertThat(filteredResult.isNull(BALANCE)).isFalse();
    assertThat(filteredResult.getInt(BALANCE)).isEqualTo(2);
    assertThat(filteredResult.getAsObject(BALANCE)).isEqualTo(2);

    assertThat(filteredResult.contains(Attribute.ID)).isFalse();
    assertThat(filteredResult.contains(Attribute.STATE)).isFalse();

    assertThat(filteredResult.contains(Attribute.VERSION)).isTrue();
    assertThat(filteredResult.isNull(Attribute.VERSION)).isFalse();
    assertThat(filteredResult.getInt(Attribute.VERSION)).isEqualTo(4);
    assertThat(filteredResult.getAsObject(Attribute.VERSION)).isEqualTo(4);

    assertThat(filteredResult.contains(Attribute.PREPARED_AT)).isFalse();
    assertThat(filteredResult.contains(Attribute.COMMITTED_AT)).isFalse();

    assertThat(filteredResult.contains(Attribute.BEFORE_PREFIX + BALANCE)).isTrue();
    assertThat(filteredResult.isNull(Attribute.BEFORE_PREFIX + BALANCE)).isFalse();
    assertThat(filteredResult.getInt(Attribute.BEFORE_PREFIX + BALANCE)).isEqualTo(7);
    assertThat(filteredResult.getAsObject(Attribute.BEFORE_PREFIX + BALANCE)).isEqualTo(7);

    assertThat(filteredResult.contains(Attribute.BEFORE_ID)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_STATE)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_VERSION)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_PREPARED_AT)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_COMMITTED_AT)).isFalse();
  }

  @Test
  public void
      withProjectionsForPartitionKey_ShouldFilterOutUnprojectedColumnsAndTransactionMetaColumns() {
    // Arrange
    FilteredResult filteredResult =
        new FilteredResult(result, Collections.singletonList(ACCOUNT_ID), TABLE_METADATA, false);

    // Act Assert
    assertThat(filteredResult.getPartitionKey()).isPresent();
    assertThat(filteredResult.getPartitionKey().get().size()).isEqualTo(1);
    assertThat(filteredResult.getPartitionKey().get().get().get(0)).isEqualTo(ACCOUNT_ID_VALUE);

    assertThat(catchThrowable(filteredResult::getClusteringKey))
        .isInstanceOf(IllegalStateException.class);

    assertThat(filteredResult.getValue(ACCOUNT_ID).isPresent()).isTrue();
    assertThat(filteredResult.getValue(ACCOUNT_ID).get()).isEqualTo(ACCOUNT_ID_VALUE);
    assertThat(filteredResult.getValue(ACCOUNT_TYPE)).isNotPresent();
    assertThat(filteredResult.getValue(BALANCE)).isNotPresent();

    assertThat(filteredResult.getValue(Attribute.ID)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.STATE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.VERSION)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.PREPARED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.COMMITTED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_PREFIX + BALANCE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_ID)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_STATE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_VERSION)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_PREPARED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_COMMITTED_AT)).isNotPresent();

    assertThat(filteredResult.getContainedColumnNames())
        .isEqualTo(new HashSet<>(Collections.singletonList(ACCOUNT_ID)));

    assertThat(filteredResult.contains(ACCOUNT_ID)).isTrue();
    assertThat(filteredResult.isNull(ACCOUNT_ID)).isFalse();
    assertThat(filteredResult.getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(filteredResult.getAsObject(ACCOUNT_ID)).isEqualTo(0);

    assertThat(filteredResult.contains(ACCOUNT_TYPE)).isFalse();

    assertThat(filteredResult.contains(BALANCE)).isFalse();

    assertThat(filteredResult.contains(Attribute.ID)).isFalse();
    assertThat(filteredResult.contains(Attribute.STATE)).isFalse();
    assertThat(filteredResult.contains(Attribute.VERSION)).isFalse();
    assertThat(filteredResult.contains(Attribute.PREPARED_AT)).isFalse();
    assertThat(filteredResult.contains(Attribute.COMMITTED_AT)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_PREFIX + BALANCE)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_ID)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_STATE)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_VERSION)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_PREPARED_AT)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_COMMITTED_AT)).isFalse();
  }

  @Test
  public void
      withProjectionsForClusteringKey_ShouldFilterOutUnprojectedColumnsAndTransactionMetaColumns() {
    // Arrange
    FilteredResult filteredResult =
        new FilteredResult(result, Collections.singletonList(ACCOUNT_TYPE), TABLE_METADATA, false);

    // Act Assert
    assertThat(catchThrowable(filteredResult::getPartitionKey))
        .isInstanceOf(IllegalStateException.class);

    assertThat(filteredResult.getClusteringKey()).isPresent();
    assertThat(filteredResult.getClusteringKey().get().size()).isEqualTo(1);

    assertThat(filteredResult.getClusteringKey().get().get().get(0)).isEqualTo(ACCOUNT_TYPE_VALUE);

    assertThat(filteredResult.getValue(ACCOUNT_ID)).isNotPresent();
    assertThat(filteredResult.getValue(ACCOUNT_TYPE).isPresent()).isTrue();
    assertThat(filteredResult.getValue(ACCOUNT_TYPE).get()).isEqualTo(ACCOUNT_TYPE_VALUE);
    assertThat(filteredResult.getValue(BALANCE)).isNotPresent();

    assertThat(filteredResult.getValue(Attribute.ID)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.STATE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.VERSION)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.PREPARED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.COMMITTED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_PREFIX + BALANCE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_ID)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_STATE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_VERSION)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_PREPARED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_COMMITTED_AT)).isNotPresent();

    assertThat(filteredResult.getContainedColumnNames())
        .isEqualTo(new HashSet<>(Collections.singletonList(ACCOUNT_TYPE)));

    assertThat(filteredResult.contains(ACCOUNT_ID)).isFalse();

    assertThat(filteredResult.contains(ACCOUNT_TYPE)).isTrue();
    assertThat(filteredResult.isNull(ACCOUNT_TYPE)).isFalse();
    assertThat(filteredResult.getInt(ACCOUNT_TYPE)).isEqualTo(1);
    assertThat(filteredResult.getAsObject(ACCOUNT_TYPE)).isEqualTo(1);

    assertThat(filteredResult.contains(BALANCE)).isFalse();

    assertThat(filteredResult.contains(Attribute.ID)).isFalse();
    assertThat(filteredResult.contains(Attribute.STATE)).isFalse();
    assertThat(filteredResult.contains(Attribute.VERSION)).isFalse();
    assertThat(filteredResult.contains(Attribute.PREPARED_AT)).isFalse();
    assertThat(filteredResult.contains(Attribute.COMMITTED_AT)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_PREFIX + BALANCE)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_ID)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_STATE)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_VERSION)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_PREPARED_AT)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_COMMITTED_AT)).isFalse();
  }

  @Test
  public void
      withProjectionsForNonPrimaryKey_ShouldFilterOutUnprojectedColumnsAndTransactionMetaColumns() {
    // Arrange
    FilteredResult filteredResult =
        new FilteredResult(result, Collections.singletonList(BALANCE), TABLE_METADATA, false);

    // Act Assert
    assertThat(catchThrowable(filteredResult::getPartitionKey))
        .isInstanceOf(IllegalStateException.class);
    assertThat(catchThrowable(filteredResult::getClusteringKey))
        .isInstanceOf(IllegalStateException.class);

    assertThat(filteredResult.getValue(ACCOUNT_ID)).isNotPresent();
    assertThat(filteredResult.getValue(ACCOUNT_TYPE)).isNotPresent();
    assertThat(filteredResult.getValue(BALANCE).isPresent()).isTrue();
    assertThat(filteredResult.getValue(BALANCE).get()).isEqualTo(BALANCE_VALUE);

    assertThat(filteredResult.getValue(Attribute.ID)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.STATE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.VERSION)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.PREPARED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.COMMITTED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_PREFIX + BALANCE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_ID)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_STATE)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_VERSION)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_PREPARED_AT)).isNotPresent();
    assertThat(filteredResult.getValue(Attribute.BEFORE_COMMITTED_AT)).isNotPresent();

    assertThat(filteredResult.getContainedColumnNames())
        .isEqualTo(new HashSet<>(Collections.singletonList(BALANCE)));

    assertThat(filteredResult.contains(ACCOUNT_ID)).isFalse();

    assertThat(filteredResult.contains(ACCOUNT_TYPE)).isFalse();

    assertThat(filteredResult.contains(BALANCE)).isTrue();
    assertThat(filteredResult.isNull(BALANCE)).isFalse();
    assertThat(filteredResult.getInt(BALANCE)).isEqualTo(2);
    assertThat(filteredResult.getAsObject(BALANCE)).isEqualTo(2);

    assertThat(filteredResult.contains(Attribute.ID)).isFalse();
    assertThat(filteredResult.contains(Attribute.STATE)).isFalse();
    assertThat(filteredResult.contains(Attribute.VERSION)).isFalse();
    assertThat(filteredResult.contains(Attribute.PREPARED_AT)).isFalse();
    assertThat(filteredResult.contains(Attribute.COMMITTED_AT)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_PREFIX + BALANCE)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_ID)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_STATE)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_VERSION)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_PREPARED_AT)).isFalse();
    assertThat(filteredResult.contains(Attribute.BEFORE_COMMITTED_AT)).isFalse();
  }

  @Test
  public void equals_SameResultGiven_WithoutProjections_ShouldReturnTrue() {
    // Arrange
    FilteredResult filteredResult =
        new FilteredResult(result, Collections.emptyList(), TABLE_METADATA, false);
    FilteredResult anotherFilterResult =
        new FilteredResult(result, Collections.emptyList(), TABLE_METADATA, false);

    // Act
    boolean isEqual = filteredResult.equals(anotherFilterResult);

    // Assert
    assertThat(isEqual).isTrue();
  }

  @Test
  public void equals_DifferentResiltGiven_WithoutProjections_ShouldReturnFalse() {
    // Arrange
    FilteredResult filteredResult =
        new FilteredResult(result, Collections.emptyList(), TABLE_METADATA, false);
    FilteredResult anotherFilterResult =
        new FilteredResult(
            new ResultImpl(Collections.emptyMap(), TABLE_METADATA),
            Collections.emptyList(),
            TABLE_METADATA,
            false);

    // Act
    boolean isEqual = filteredResult.equals(anotherFilterResult);

    // Assert
    assertThat(isEqual).isFalse();
  }

  @Test
  public void equals_SameResultGiven_WithProjections_ShouldReturnTrue() {
    // Arrange
    FilteredResult filteredResult =
        new FilteredResult(result, Collections.singletonList(BALANCE), TABLE_METADATA, false);
    FilteredResult anotherFilterResult =
        new FilteredResult(result, Collections.singletonList(BALANCE), TABLE_METADATA, false);

    // Act
    boolean isEqual = filteredResult.equals(anotherFilterResult);

    // Assert
    assertThat(isEqual).isTrue();
  }

  @Test
  public void equals_ResultImplWithSameValuesGiven_WithoutProjections_ShouldReturnTrue() {
    // Arrange
    Result filteredResult =
        new FilteredResult(result, Collections.emptyList(), TABLE_METADATA, false);
    Result anotherResult =
        new ResultImpl(
            ImmutableMap.of(
                ACCOUNT_ID,
                ScalarDbUtils.toColumn(ACCOUNT_ID_VALUE),
                ACCOUNT_TYPE,
                ScalarDbUtils.toColumn(ACCOUNT_TYPE_VALUE),
                BALANCE,
                ScalarDbUtils.toColumn(BALANCE_VALUE)),
            TABLE_METADATA);

    // Act
    boolean isEqual = filteredResult.equals(anotherResult);

    // Assert
    assertThat(isEqual).isTrue();
  }

  @Test
  public void equals_ResultImplWithSameValuesGiven_WithProjections_ShouldReturnFalse() {
    // Arrange
    Result filteredResult =
        new FilteredResult(result, Collections.singletonList(BALANCE), TABLE_METADATA, false);

    // Act
    boolean isEqual = filteredResult.equals(result);

    // Assert
    assertThat(isEqual).isFalse();
  }

  @Test
  public void equals_ResultImplWithDifferentValuesGiven_WithProjections_ShouldReturnTrue() {
    // Arrange
    Result filteredResult =
        new FilteredResult(result, Collections.singletonList(BALANCE), TABLE_METADATA, false);
    Result anotherResult =
        new ResultImpl(
            ImmutableMap.of(BALANCE, ScalarDbUtils.toColumn(BALANCE_VALUE)), TABLE_METADATA);

    // Act
    boolean isEqual = filteredResult.equals(anotherResult);

    // Assert
    assertThat(isEqual).isTrue();
  }
}
