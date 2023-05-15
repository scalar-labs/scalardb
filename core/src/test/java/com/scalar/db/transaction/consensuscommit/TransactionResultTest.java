package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import com.scalar.db.util.ScalarDbUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class TransactionResultTest {
  private static final String ANY_ID_1 = "id1";
  private static final String ANY_ID_2 = "id2";
  private static final long ANY_TIME_1 = 100;
  private static final long ANY_TIME_2 = 200;
  private static final long ANY_TIME_3 = 300;
  private static final long ANY_TIME_4 = 400;
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final int ANY_INT_1 = 100;
  private static final int ANY_INT_2 = 200;
  private static final int ANY_VERSION_1 = 1;
  private static final int ANY_VERSION_2 = 2;

  private static final TableMetadata TABLE_METADATA =
      ConsensusCommitUtils.buildTransactionTableMetadata(
          TableMetadata.newBuilder()
              .addColumn(ANY_NAME_1, DataType.TEXT)
              .addColumn(ANY_NAME_2, DataType.TEXT)
              .addColumn(ANY_NAME_3, DataType.INT)
              .addPartitionKey(ANY_NAME_1)
              .addClusteringKey(ANY_NAME_2)
              .build());

  private TransactionResult prepareResult() {
    return new TransactionResult(
        new ResultImpl(
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
                .put(
                    Attribute.VERSION,
                    ScalarDbUtils.toColumn(Attribute.toVersionValue(ANY_VERSION_2)))
                .put(
                    Attribute.BEFORE_PREFIX + ANY_NAME_3,
                    IntColumn.of(Attribute.BEFORE_PREFIX + ANY_NAME_3, ANY_INT_1))
                .put(
                    Attribute.BEFORE_ID,
                    ScalarDbUtils.toColumn(Attribute.toBeforeIdValue(ANY_ID_1)))
                .put(
                    Attribute.BEFORE_PREPARED_AT,
                    ScalarDbUtils.toColumn(Attribute.toBeforePreparedAtValue(ANY_TIME_1)))
                .put(
                    Attribute.BEFORE_COMMITTED_AT,
                    ScalarDbUtils.toColumn(Attribute.toBeforeCommittedAtValue(ANY_TIME_2)))
                .put(
                    Attribute.BEFORE_STATE,
                    ScalarDbUtils.toColumn(
                        Attribute.toBeforeStateValue(TransactionState.COMMITTED)))
                .put(
                    Attribute.BEFORE_VERSION,
                    ScalarDbUtils.toColumn(Attribute.toBeforeVersionValue(ANY_VERSION_1)))
                .build(),
            TABLE_METADATA));
  }

  private TransactionResult prepareResultWithNullMetadata() {
    return new TransactionResult(
        new ResultImpl(
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
                .put(
                    Attribute.BEFORE_PREPARED_AT, BigIntColumn.ofNull(Attribute.BEFORE_PREPARED_AT))
                .put(
                    Attribute.BEFORE_COMMITTED_AT,
                    BigIntColumn.ofNull(Attribute.BEFORE_COMMITTED_AT))
                .put(Attribute.BEFORE_STATE, IntColumn.ofNull(Attribute.BEFORE_STATE))
                .put(Attribute.BEFORE_VERSION, IntColumn.ofNull(Attribute.BEFORE_VERSION))
                .build(),
            TABLE_METADATA));
  }

  @Test
  public void getPartitionKey_ResultGiven_ShouldReturnCorrectKey() {
    // Arrange
    TransactionResult result = prepareResult();

    // Act
    Optional<Key> actual = result.getPartitionKey();

    // Assert
    assertThat(actual).isEqualTo(Optional.of(new Key(ANY_NAME_1, ANY_TEXT_1)));
  }

  @Test
  public void getClusteringKey_ResultGiven_ShouldReturnCorrectKey() {
    // Arrange
    TransactionResult result = prepareResult();

    // Act
    Optional<Key> actual = result.getClusteringKey();

    // Assert
    assertThat(actual).isEqualTo(Optional.of(new Key(ANY_NAME_2, ANY_TEXT_2)));
  }

  @Test
  public void getValue_ResultGiven_ShouldReturnCorrectValue() {
    // Arrange
    TransactionResult result = prepareResult();

    // Act Assert
    assertThat(result.getValue(ANY_NAME_1))
        .isEqualTo(Optional.of(new TextValue(ANY_NAME_1, ANY_TEXT_1)));
    assertThat(result.getValue(ANY_NAME_2))
        .isEqualTo(Optional.of(new TextValue(ANY_NAME_2, ANY_TEXT_2)));
    assertThat(result.getValue(ANY_NAME_3))
        .isEqualTo(Optional.of(new IntValue(ANY_NAME_3, ANY_INT_2)));
    assertThat(result.getValue(Attribute.ID)).isEqualTo(Optional.of(Attribute.toIdValue(ANY_ID_2)));
    assertThat(result.getValue(Attribute.PREPARED_AT))
        .isEqualTo(Optional.of(Attribute.toPreparedAtValue(ANY_TIME_3)));
    assertThat(result.getValue(Attribute.COMMITTED_AT))
        .isEqualTo(Optional.of(Attribute.toCommittedAtValue(ANY_TIME_4)));
    assertThat(result.getValue(Attribute.STATE))
        .isEqualTo(Optional.of(Attribute.toStateValue(TransactionState.COMMITTED)));
    assertThat(result.getValue(Attribute.VERSION))
        .isEqualTo(Optional.of(Attribute.toVersionValue(ANY_VERSION_2)));
    assertThat(result.getValue(Attribute.BEFORE_PREFIX + ANY_NAME_3))
        .isEqualTo(Optional.of(new IntValue(Attribute.BEFORE_PREFIX + ANY_NAME_3, ANY_INT_1)));
    assertThat(result.getValue(Attribute.BEFORE_ID))
        .isEqualTo(Optional.of(Attribute.toBeforeIdValue(ANY_ID_1)));
    assertThat(result.getValue(Attribute.BEFORE_PREPARED_AT))
        .isEqualTo(Optional.of(Attribute.toBeforePreparedAtValue(ANY_TIME_1)));
    assertThat(result.getValue(Attribute.BEFORE_COMMITTED_AT))
        .isEqualTo(Optional.of(Attribute.toBeforeCommittedAtValue(ANY_TIME_2)));
    assertThat(result.getValue(Attribute.BEFORE_STATE))
        .isEqualTo(Optional.of(Attribute.toBeforeStateValue(TransactionState.COMMITTED)));
    assertThat(result.getValue(Attribute.BEFORE_VERSION))
        .isEqualTo(Optional.of(Attribute.toBeforeVersionValue(ANY_VERSION_1)));

    assertThat(result.getContainedColumnNames())
        .isEqualTo(
            new HashSet<>(
                Arrays.asList(
                    ANY_NAME_1,
                    ANY_NAME_2,
                    ANY_NAME_3,
                    Attribute.ID,
                    Attribute.PREPARED_AT,
                    Attribute.COMMITTED_AT,
                    Attribute.STATE,
                    Attribute.VERSION,
                    Attribute.BEFORE_PREFIX + ANY_NAME_3,
                    Attribute.BEFORE_ID,
                    Attribute.BEFORE_PREPARED_AT,
                    Attribute.BEFORE_COMMITTED_AT,
                    Attribute.BEFORE_STATE,
                    Attribute.BEFORE_VERSION)));

    assertThat(result.contains(ANY_NAME_1)).isTrue();
    assertThat(result.isNull(ANY_NAME_1)).isFalse();
    assertThat(result.getText(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);
    assertThat(result.getAsObject(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);

    assertThat(result.contains(ANY_NAME_2)).isTrue();
    assertThat(result.isNull(ANY_NAME_2)).isFalse();
    assertThat(result.getText(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);
    assertThat(result.getAsObject(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);

    assertThat(result.contains(ANY_NAME_3)).isTrue();
    assertThat(result.isNull(ANY_NAME_3)).isFalse();
    assertThat(result.getInt(ANY_NAME_3)).isEqualTo(ANY_INT_2);
    assertThat(result.getAsObject(ANY_NAME_3)).isEqualTo(ANY_INT_2);

    assertThat(result.contains(Attribute.ID)).isTrue();
    assertThat(result.isNull(Attribute.ID)).isFalse();
    assertThat(result.getText(Attribute.ID)).isEqualTo(ANY_ID_2);
    assertThat(result.getAsObject(Attribute.ID)).isEqualTo(ANY_ID_2);

    assertThat(result.contains(Attribute.PREPARED_AT)).isTrue();
    assertThat(result.isNull(Attribute.PREPARED_AT)).isFalse();
    assertThat(result.getBigInt(Attribute.PREPARED_AT)).isEqualTo(ANY_TIME_3);
    assertThat(result.getAsObject(Attribute.PREPARED_AT)).isEqualTo(ANY_TIME_3);

    assertThat(result.contains(Attribute.COMMITTED_AT)).isTrue();
    assertThat(result.isNull(Attribute.COMMITTED_AT)).isFalse();
    assertThat(result.getBigInt(Attribute.COMMITTED_AT)).isEqualTo(ANY_TIME_4);
    assertThat(result.getAsObject(Attribute.COMMITTED_AT)).isEqualTo(ANY_TIME_4);

    assertThat(result.contains(Attribute.STATE)).isTrue();
    assertThat(result.isNull(Attribute.STATE)).isFalse();
    assertThat(result.getInt(Attribute.STATE)).isEqualTo(TransactionState.COMMITTED.get());
    assertThat(result.getAsObject(Attribute.STATE)).isEqualTo(TransactionState.COMMITTED.get());

    assertThat(result.contains(Attribute.VERSION)).isTrue();
    assertThat(result.isNull(Attribute.VERSION)).isFalse();
    assertThat(result.getInt(Attribute.VERSION)).isEqualTo(ANY_VERSION_2);
    assertThat(result.getAsObject(Attribute.VERSION)).isEqualTo(ANY_VERSION_2);

    assertThat(result.contains(Attribute.BEFORE_PREFIX + ANY_NAME_3)).isTrue();
    assertThat(result.isNull(Attribute.BEFORE_PREFIX + ANY_NAME_3)).isFalse();
    assertThat(result.getInt(Attribute.BEFORE_PREFIX + ANY_NAME_3)).isEqualTo(ANY_INT_1);
    assertThat(result.getAsObject(Attribute.BEFORE_PREFIX + ANY_NAME_3)).isEqualTo(ANY_INT_1);

    assertThat(result.contains(Attribute.BEFORE_ID)).isTrue();
    assertThat(result.isNull(Attribute.BEFORE_ID)).isFalse();
    assertThat(result.getText(Attribute.BEFORE_ID)).isEqualTo(ANY_ID_1);
    assertThat(result.getAsObject(Attribute.BEFORE_ID)).isEqualTo(ANY_ID_1);

    assertThat(result.contains(Attribute.BEFORE_PREPARED_AT)).isTrue();
    assertThat(result.isNull(Attribute.BEFORE_PREPARED_AT)).isFalse();
    assertThat(result.getBigInt(Attribute.BEFORE_PREPARED_AT)).isEqualTo(ANY_TIME_1);
    assertThat(result.getAsObject(Attribute.BEFORE_PREPARED_AT)).isEqualTo(ANY_TIME_1);

    assertThat(result.contains(Attribute.BEFORE_COMMITTED_AT)).isTrue();
    assertThat(result.isNull(Attribute.BEFORE_COMMITTED_AT)).isFalse();
    assertThat(result.getBigInt(Attribute.BEFORE_COMMITTED_AT)).isEqualTo(ANY_TIME_2);
    assertThat(result.getAsObject(Attribute.BEFORE_COMMITTED_AT)).isEqualTo(ANY_TIME_2);

    assertThat(result.contains(Attribute.BEFORE_STATE)).isTrue();
    assertThat(result.isNull(Attribute.BEFORE_STATE)).isFalse();
    assertThat(result.getInt(Attribute.BEFORE_STATE)).isEqualTo(TransactionState.COMMITTED.get());
    assertThat(result.getAsObject(Attribute.BEFORE_STATE))
        .isEqualTo(TransactionState.COMMITTED.get());

    assertThat(result.contains(Attribute.BEFORE_VERSION)).isTrue();
    assertThat(result.isNull(Attribute.BEFORE_VERSION)).isFalse();
    assertThat(result.getInt(Attribute.BEFORE_VERSION)).isEqualTo(ANY_VERSION_1);
    assertThat(result.getAsObject(Attribute.BEFORE_VERSION)).isEqualTo(ANY_VERSION_1);
  }

  @Test
  public void getValues_ResultGiven_ShouldReturnCorrectValues() {
    // Arrange
    Result given = prepareResult();
    TransactionResult result = prepareResult();

    // Act
    Map<String, Value<?>> values = result.getValues();

    // Assert
    assertThat(values.size()).isEqualTo(given.getValues().size());
    given.getValues().forEach((k, v) -> assertThat(values.get(v.getName())).isEqualTo(v));
  }

  @Test
  public void equals_SameResultGiven_ShouldReturnTrue() {
    // Arrange
    TransactionResult result = prepareResult();
    TransactionResult another = prepareResult();

    // Act
    boolean isEqual = result.equals(another);

    // Assert
    assertThat(isEqual).isTrue();
  }

  @Test
  public void equals_DifferentResultGiven_ShouldReturnFalse() {
    // Arrange
    TransactionResult result = prepareResult();
    TransactionResult another = new TransactionResult(mock(Result.class));

    // Act
    boolean isEqual = result.equals(another);

    // Assert
    assertThat(isEqual).isFalse();
  }

  @Test
  public void getTransactionId_ResultGiven_ShouldReturnCorrectId() {
    // Arrange
    TransactionResult result = prepareResult();

    // Act
    String id = result.getId();

    // Assert
    assertThat(id).isEqualTo(ANY_ID_2);
  }

  @Test
  public void getTransactionState_ResultGiven_ShouldReturnCorrectState() {
    // Arrange
    TransactionResult result = prepareResult();

    // Act
    TransactionState state = result.getState();

    // Assert
    assertThat(state).isEqualTo(TransactionState.COMMITTED);
  }

  @Test
  public void getTransactionState_ResultWithNullMetadataGiven_ShouldReturnCorrectState() {
    // Arrange
    TransactionResult result = prepareResultWithNullMetadata();

    // Act
    TransactionState state = result.getState();

    // Assert
    assertThat(state).isEqualTo(TransactionState.COMMITTED);
  }

  @Test
  public void getTransactionVersion_ResultGiven_ShouldReturnCorrectVersion() {
    // Arrange
    TransactionResult result = prepareResult();

    // Act
    int version = result.getVersion();

    // Assert
    assertThat(version).isEqualTo(ANY_VERSION_2);
  }

  @Test
  public void isCommitted_ResultGiven_ShouldReturnCommitted() {
    // Arrange
    TransactionResult result = prepareResult();

    // Act
    boolean isCommitted = result.isCommitted();

    // Assert
    assertThat(isCommitted).isTrue();
  }

  @Test
  public void equals_ResultImplWithSameValuesGiven_ShouldReturnTrue() {
    // Arrange
    Result r1 =
        new ResultImpl(
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
                .put(
                    Attribute.VERSION,
                    ScalarDbUtils.toColumn(Attribute.toVersionValue(ANY_VERSION_2)))
                .put(
                    Attribute.BEFORE_PREFIX + ANY_NAME_3,
                    IntColumn.of(Attribute.BEFORE_PREFIX + ANY_NAME_3, ANY_INT_1))
                .put(
                    Attribute.BEFORE_ID,
                    ScalarDbUtils.toColumn(Attribute.toBeforeIdValue(ANY_ID_1)))
                .put(
                    Attribute.BEFORE_PREPARED_AT,
                    ScalarDbUtils.toColumn(Attribute.toBeforePreparedAtValue(ANY_TIME_1)))
                .put(
                    Attribute.BEFORE_COMMITTED_AT,
                    ScalarDbUtils.toColumn(Attribute.toBeforeCommittedAtValue(ANY_TIME_2)))
                .put(
                    Attribute.BEFORE_STATE,
                    ScalarDbUtils.toColumn(
                        Attribute.toBeforeStateValue(TransactionState.COMMITTED)))
                .put(
                    Attribute.BEFORE_VERSION,
                    ScalarDbUtils.toColumn(Attribute.toBeforeVersionValue(ANY_VERSION_1)))
                .build(),
            TABLE_METADATA);
    Result r2 = new TransactionResult(r1);

    // Act Assert
    assertThat(r1.equals(r2)).isTrue();
  }

  @Test
  public void equals_ResultImplWithDifferentValuesGiven_ShouldReturnFalse() {
    // Arrange
    Result r1 =
        new ResultImpl(
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
                .put(
                    Attribute.VERSION,
                    ScalarDbUtils.toColumn(Attribute.toVersionValue(ANY_VERSION_2)))
                .put(
                    Attribute.BEFORE_PREFIX + ANY_NAME_3,
                    IntColumn.of(Attribute.BEFORE_PREFIX + ANY_NAME_3, ANY_INT_1))
                .put(
                    Attribute.BEFORE_ID,
                    ScalarDbUtils.toColumn(Attribute.toBeforeIdValue(ANY_ID_1)))
                .put(
                    Attribute.BEFORE_PREPARED_AT,
                    ScalarDbUtils.toColumn(Attribute.toBeforePreparedAtValue(ANY_TIME_1)))
                .put(
                    Attribute.BEFORE_COMMITTED_AT,
                    ScalarDbUtils.toColumn(Attribute.toBeforeCommittedAtValue(ANY_TIME_2)))
                .put(
                    Attribute.BEFORE_STATE,
                    ScalarDbUtils.toColumn(
                        Attribute.toBeforeStateValue(TransactionState.COMMITTED)))
                .put(
                    Attribute.BEFORE_VERSION,
                    ScalarDbUtils.toColumn(Attribute.toBeforeVersionValue(ANY_VERSION_1)))
                .build(),
            TABLE_METADATA);
    Map<String, Column<?>> emptyValues = Collections.emptyMap();
    Result r2 = new TransactionResult(new ResultImpl(emptyValues, TABLE_METADATA));

    // Act Assert
    assertThat(r1.equals(r2)).isFalse();
  }
}
