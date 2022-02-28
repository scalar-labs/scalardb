package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import com.scalar.db.util.ResultImpl;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class FilteredResultTest {

  private static final String ACCOUNT_ID = "account_id";
  private static final String ACCOUNT_TYPE = "account_type";
  private static final String BALANCE = "balance";

  private static final TableMetadata TABLE_METADATA =
      ConsensusCommitUtils.buildTransactionalTableMetadata(
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

  @Before
  public void setUp() {
    // Arrange
    Map<String, Optional<Value<?>>> values =
        ImmutableMap.<String, Optional<Value<?>>>builder()
            .put(ACCOUNT_ID, Optional.of(ACCOUNT_ID_VALUE))
            .put(ACCOUNT_TYPE, Optional.of(ACCOUNT_TYPE_VALUE))
            .put(BALANCE, Optional.of(BALANCE_VALUE))
            .put(Attribute.ID, Optional.of(ID_VALUE))
            .put(Attribute.STATE, Optional.of(STATE_VALUE))
            .put(Attribute.VERSION, Optional.of(VERSION_VALUE))
            .put(Attribute.PREPARED_AT, Optional.of(PREPARED_AT_VALUE))
            .put(Attribute.COMMITTED_AT, Optional.of(COMMITTED_AT_VALUE))
            .put(Attribute.BEFORE_PREFIX + BALANCE, Optional.of(BEFORE_BALANCE_VALUE))
            .put(Attribute.BEFORE_ID, Optional.of(BEFORE_ID_VALUE))
            .put(Attribute.BEFORE_STATE, Optional.of(BEFORE_STATE_VALUE))
            .put(Attribute.BEFORE_VERSION, Optional.of(BEFORE_VERSION_VALUE))
            .put(Attribute.BEFORE_PREPARED_AT, Optional.of(BEFORE_PREPARED_AT_VALUE))
            .put(Attribute.BEFORE_COMMITTED_AT, Optional.of(BEFORE_COMMITTED_AT_VALUE))
            .build();

    result = new ResultImpl(values, TABLE_METADATA);
  }

  @Test
  public void withoutProjections_ShouldFilterOutTransactionMetaColumns() {
    // Arrange
    FilteredResult filteredResult =
        new FilteredResult(result, Collections.emptyList(), TABLE_METADATA);

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
  public void withProjections_ShouldFilterOutUnprojectedColumnsAndTransactionMetaColumns() {
    // Arrange
    FilteredResult filteredResult =
        new FilteredResult(result, Arrays.asList(ACCOUNT_ID, BALANCE), TABLE_METADATA);

    // Act Assert
    assertThat(filteredResult.getPartitionKey()).isPresent();
    assertThat(filteredResult.getPartitionKey().get().size()).isEqualTo(1);
    assertThat(filteredResult.getPartitionKey().get().get().get(0)).isEqualTo(ACCOUNT_ID_VALUE);

    assertThat(filteredResult.getClusteringKey()).isNotPresent();

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
  public void
      withProjectionsForPartitionKey_ShouldFilterOutUnprojectedColumnsAndTransactionMetaColumns() {
    // Arrange
    FilteredResult filteredResult =
        new FilteredResult(result, Collections.singletonList(ACCOUNT_ID), TABLE_METADATA);

    // Act Assert
    assertThat(filteredResult.getPartitionKey()).isPresent();
    assertThat(filteredResult.getPartitionKey().get().size()).isEqualTo(1);
    assertThat(filteredResult.getPartitionKey().get().get().get(0)).isEqualTo(ACCOUNT_ID_VALUE);

    assertThat(filteredResult.getClusteringKey()).isNotPresent();

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
        new FilteredResult(result, Collections.singletonList(ACCOUNT_TYPE), TABLE_METADATA);

    // Act Assert
    assertThat(filteredResult.getPartitionKey()).isNotPresent();

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
        new FilteredResult(result, Collections.singletonList(BALANCE), TABLE_METADATA);

    // Act Assert
    assertThat(filteredResult.getPartitionKey()).isNotPresent();
    assertThat(filteredResult.getClusteringKey()).isNotPresent();

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
        new FilteredResult(result, Collections.emptyList(), TABLE_METADATA);
    FilteredResult anotherFilterResult =
        new FilteredResult(result, Collections.emptyList(), TABLE_METADATA);

    // Act
    boolean isEqual = filteredResult.equals(anotherFilterResult);

    // Assert
    assertThat(isEqual).isTrue();
  }

  @Test
  public void equals_DifferentResiltGiven_WithoutProjections_ShouldReturnFalse() {
    // Arrange
    FilteredResult filteredResult =
        new FilteredResult(result, Collections.emptyList(), TABLE_METADATA);
    FilteredResult anotherFilterResult =
        new FilteredResult(
            new ResultImpl(Collections.emptyMap(), TABLE_METADATA),
            Collections.emptyList(),
            TABLE_METADATA);

    // Act
    boolean isEqual = filteredResult.equals(anotherFilterResult);

    // Assert
    assertThat(isEqual).isFalse();
  }

  @Test
  public void equals_SameResultGiven_WithProjections_ShouldReturnTrue() {
    // Arrange
    FilteredResult filteredResult =
        new FilteredResult(result, Collections.singletonList(BALANCE), TABLE_METADATA);
    FilteredResult anotherFilterResult =
        new FilteredResult(result, Collections.singletonList(BALANCE), TABLE_METADATA);

    // Act
    boolean isEqual = filteredResult.equals(anotherFilterResult);

    // Assert
    assertThat(isEqual).isTrue();
  }

  @Test
  public void equals_ResultImplWithSameValuesGiven_WithoutProjections_ShouldReturnTrue() {
    // Arrange
    Result filteredResult = new FilteredResult(result, Collections.emptyList(), TABLE_METADATA);
    Result anotherResult =
        new ResultImpl(
            ImmutableMap.of(
                ACCOUNT_ID,
                Optional.of(ACCOUNT_ID_VALUE),
                ACCOUNT_TYPE,
                Optional.of(ACCOUNT_TYPE_VALUE),
                BALANCE,
                Optional.of(BALANCE_VALUE)),
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
        new FilteredResult(result, Collections.singletonList(BALANCE), TABLE_METADATA);

    // Act
    boolean isEqual = filteredResult.equals(result);

    // Assert
    assertThat(isEqual).isFalse();
  }

  @Test
  public void equals_ResultImplWithDifferentValuesGiven_WithProjections_ShouldReturnTrue() {
    // Arrange
    Result filteredResult =
        new FilteredResult(result, Collections.singletonList(BALANCE), TABLE_METADATA);
    Result anotherResult =
        new ResultImpl(ImmutableMap.of(BALANCE, Optional.of(BALANCE_VALUE)), TABLE_METADATA);

    // Act
    boolean isEqual = filteredResult.equals(anotherResult);

    // Assert
    assertThat(isEqual).isTrue();
  }
}
