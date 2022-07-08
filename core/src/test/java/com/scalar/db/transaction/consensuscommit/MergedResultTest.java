package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.ResultImpl;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MergedResultTest {

  private static final String ANY_ID_1 = "id1";
  private static final String ANY_ID_2 = "id2";
  private static final long ANY_TIME_1 = 100;
  private static final long ANY_TIME_2 = 200;
  private static final long ANY_TIME_3 = 300;
  private static final long ANY_TIME_4 = 400;
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_NAME_4 = "name4";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final String ANY_TEXT_4 = "text4";
  private static final int ANY_INT_1 = 100;
  private static final int ANY_INT_2 = 200;
  private static final int ANY_INT_3 = 300;
  private static final int ANY_VERSION_1 = 1;
  private static final int ANY_VERSION_2 = 2;

  private static final TableMetadata TABLE_METADATA =
      ConsensusCommitUtils.buildTransactionTableMetadata(
          TableMetadata.newBuilder()
              .addColumn(ANY_NAME_1, DataType.TEXT)
              .addColumn(ANY_NAME_2, DataType.TEXT)
              .addColumn(ANY_NAME_3, DataType.INT)
              .addColumn(ANY_NAME_4, DataType.TEXT)
              .addPartitionKey(ANY_NAME_1)
              .addClusteringKey(ANY_NAME_2)
              .build());

  private TransactionResult result;

  @BeforeEach
  public void setUp() {
    // Arrange
    result =
        new TransactionResult(
            new ResultImpl(
                ImmutableMap.<String, Column<?>>builder()
                    .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1))
                    .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_2))
                    .put(ANY_NAME_3, IntColumn.of(ANY_NAME_3, ANY_INT_2))
                    .put(ANY_NAME_4, TextColumn.of(ANY_NAME_4, ANY_TEXT_3))
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
                        Attribute.BEFORE_PREFIX + ANY_NAME_4,
                        TextColumn.of(Attribute.BEFORE_PREFIX + ANY_NAME_4, ANY_TEXT_4))
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

  @Test
  public void getPartitionKey_ResultAndPutGiven_ShouldReturnCorrectKey() {
    // Arrange
    Put put =
        new Put(new Key(ANY_NAME_1, ANY_TEXT_1), new Key(ANY_NAME_2, ANY_TEXT_2))
            .withValue(ANY_NAME_3, ANY_INT_3);

    MergedResult mergedResult = new MergedResult(Optional.of(result), put, TABLE_METADATA);

    // Act
    Optional<Key> actual = mergedResult.getPartitionKey();

    // Assert
    assertThat(actual).isEqualTo(Optional.of(new Key(ANY_NAME_1, ANY_TEXT_1)));
  }

  @Test
  public void getPartitionKey_OnlyPutGiven_ShouldReturnCorrectKey() {
    // Arrange
    Put put =
        new Put(new Key(ANY_NAME_1, ANY_TEXT_1), new Key(ANY_NAME_2, ANY_TEXT_2))
            .withValue(ANY_NAME_3, ANY_INT_3);

    MergedResult mergedResult = new MergedResult(Optional.empty(), put, TABLE_METADATA);

    // Act
    Optional<Key> actual = mergedResult.getPartitionKey();

    // Assert
    assertThat(actual).isEqualTo(Optional.of(new Key(ANY_NAME_1, ANY_TEXT_1)));
  }

  @Test
  public void getClusteringKey_ResultAndPutGiven_ShouldReturnCorrectKey() {
    // Arrange
    Put put =
        new Put(new Key(ANY_NAME_1, ANY_TEXT_1), new Key(ANY_NAME_2, ANY_TEXT_2))
            .withValue(ANY_NAME_3, ANY_INT_3);

    MergedResult mergedResult = new MergedResult(Optional.of(result), put, TABLE_METADATA);

    // Act
    Optional<Key> actual = mergedResult.getClusteringKey();

    // Assert
    assertThat(actual).isEqualTo(Optional.of(new Key(ANY_NAME_2, ANY_TEXT_2)));
  }

  @Test
  public void getClusteringKey_OnlyPutGiven_ShouldReturnCorrectKey() {
    // Arrange
    Put put =
        new Put(new Key(ANY_NAME_1, ANY_TEXT_1), new Key(ANY_NAME_2, ANY_TEXT_2))
            .withValue(ANY_NAME_3, ANY_INT_3);

    MergedResult mergedResult = new MergedResult(Optional.empty(), put, TABLE_METADATA);

    // Act
    Optional<Key> actual = mergedResult.getClusteringKey();

    // Assert
    assertThat(actual).isEqualTo(Optional.of(new Key(ANY_NAME_2, ANY_TEXT_2)));
  }

  @Test
  public void getValue_ResultAndPutGiven_ShouldReturnMergedValue() {
    // Arrange
    Put put =
        new Put(new Key(ANY_NAME_1, ANY_TEXT_1), new Key(ANY_NAME_2, ANY_TEXT_2))
            .withValue(ANY_NAME_3, ANY_INT_3);

    MergedResult mergedResult = new MergedResult(Optional.of(result), put, TABLE_METADATA);

    // Act Assert
    assertThat(mergedResult.getValue(ANY_NAME_1))
        .isEqualTo(Optional.of(new TextValue(ANY_NAME_1, ANY_TEXT_1)));
    assertThat(mergedResult.getValue(ANY_NAME_2))
        .isEqualTo(Optional.of(new TextValue(ANY_NAME_2, ANY_TEXT_2)));
    assertThat(mergedResult.getValue(ANY_NAME_3))
        .isEqualTo(Optional.of(new IntValue(ANY_NAME_3, ANY_INT_3)));
    assertThat(mergedResult.getValue(ANY_NAME_4))
        .isEqualTo(Optional.of(new TextValue(ANY_NAME_4, ANY_TEXT_3)));
    assertThat(mergedResult.getValue(Attribute.ID))
        .isEqualTo(Optional.of(Attribute.toIdValue(ANY_ID_2)));
    assertThat(mergedResult.getValue(Attribute.PREPARED_AT))
        .isEqualTo(Optional.of(Attribute.toPreparedAtValue(ANY_TIME_3)));
    assertThat(mergedResult.getValue(Attribute.COMMITTED_AT))
        .isEqualTo(Optional.of(Attribute.toCommittedAtValue(ANY_TIME_4)));
    assertThat(mergedResult.getValue(Attribute.STATE))
        .isEqualTo(Optional.of(Attribute.toStateValue(TransactionState.COMMITTED)));
    assertThat(mergedResult.getValue(Attribute.VERSION))
        .isEqualTo(Optional.of(Attribute.toVersionValue(ANY_VERSION_2)));
    assertThat(mergedResult.getValue(Attribute.BEFORE_PREFIX + ANY_NAME_3))
        .isEqualTo(Optional.of(new IntValue(Attribute.BEFORE_PREFIX + ANY_NAME_3, ANY_INT_1)));
    assertThat(mergedResult.getValue(Attribute.BEFORE_ID))
        .isEqualTo(Optional.of(Attribute.toBeforeIdValue(ANY_ID_1)));
    assertThat(mergedResult.getValue(Attribute.BEFORE_PREPARED_AT))
        .isEqualTo(Optional.of(Attribute.toBeforePreparedAtValue(ANY_TIME_1)));
    assertThat(mergedResult.getValue(Attribute.BEFORE_COMMITTED_AT))
        .isEqualTo(Optional.of(Attribute.toBeforeCommittedAtValue(ANY_TIME_2)));
    assertThat(mergedResult.getValue(Attribute.BEFORE_STATE))
        .isEqualTo(Optional.of(Attribute.toBeforeStateValue(TransactionState.COMMITTED)));
    assertThat(mergedResult.getValue(Attribute.BEFORE_VERSION))
        .isEqualTo(Optional.of(Attribute.toBeforeVersionValue(ANY_VERSION_1)));

    assertThat(mergedResult.getContainedColumnNames())
        .isEqualTo(
            new HashSet<>(
                Arrays.asList(
                    ANY_NAME_1,
                    ANY_NAME_2,
                    ANY_NAME_3,
                    ANY_NAME_4,
                    Attribute.ID,
                    Attribute.PREPARED_AT,
                    Attribute.COMMITTED_AT,
                    Attribute.STATE,
                    Attribute.VERSION,
                    Attribute.BEFORE_PREFIX + ANY_NAME_3,
                    Attribute.BEFORE_PREFIX + ANY_NAME_4,
                    Attribute.BEFORE_ID,
                    Attribute.BEFORE_PREPARED_AT,
                    Attribute.BEFORE_COMMITTED_AT,
                    Attribute.BEFORE_STATE,
                    Attribute.BEFORE_VERSION)));

    assertThat(mergedResult.contains(ANY_NAME_1)).isTrue();
    assertThat(mergedResult.isNull(ANY_NAME_1)).isFalse();
    assertThat(mergedResult.getText(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);
    assertThat(mergedResult.getAsObject(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);

    assertThat(mergedResult.contains(ANY_NAME_2)).isTrue();
    assertThat(mergedResult.isNull(ANY_NAME_2)).isFalse();
    assertThat(mergedResult.getText(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);
    assertThat(mergedResult.getAsObject(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);

    assertThat(mergedResult.contains(ANY_NAME_3)).isTrue();
    assertThat(mergedResult.isNull(ANY_NAME_3)).isFalse();
    assertThat(mergedResult.getInt(ANY_NAME_3)).isEqualTo(ANY_INT_3);
    assertThat(mergedResult.getAsObject(ANY_NAME_3)).isEqualTo(ANY_INT_3);

    assertThat(mergedResult.contains(ANY_NAME_4)).isTrue();
    assertThat(mergedResult.isNull(ANY_NAME_4)).isFalse();
    assertThat(mergedResult.getText(ANY_NAME_4)).isEqualTo(ANY_TEXT_3);
    assertThat(mergedResult.getAsObject(ANY_NAME_4)).isEqualTo(ANY_TEXT_3);

    assertThat(mergedResult.contains(Attribute.ID)).isTrue();
    assertThat(mergedResult.isNull(Attribute.ID)).isFalse();
    assertThat(mergedResult.getText(Attribute.ID)).isEqualTo(ANY_ID_2);
    assertThat(mergedResult.getAsObject(Attribute.ID)).isEqualTo(ANY_ID_2);

    assertThat(mergedResult.contains(Attribute.PREPARED_AT)).isTrue();
    assertThat(mergedResult.isNull(Attribute.PREPARED_AT)).isFalse();
    assertThat(mergedResult.getBigInt(Attribute.PREPARED_AT)).isEqualTo(ANY_TIME_3);
    assertThat(mergedResult.getAsObject(Attribute.PREPARED_AT)).isEqualTo(ANY_TIME_3);

    assertThat(mergedResult.contains(Attribute.COMMITTED_AT)).isTrue();
    assertThat(mergedResult.isNull(Attribute.COMMITTED_AT)).isFalse();
    assertThat(mergedResult.getBigInt(Attribute.COMMITTED_AT)).isEqualTo(ANY_TIME_4);
    assertThat(mergedResult.getAsObject(Attribute.COMMITTED_AT)).isEqualTo(ANY_TIME_4);

    assertThat(mergedResult.contains(Attribute.STATE)).isTrue();
    assertThat(mergedResult.isNull(Attribute.STATE)).isFalse();
    assertThat(mergedResult.getInt(Attribute.STATE)).isEqualTo(TransactionState.COMMITTED.get());
    assertThat(mergedResult.getAsObject(Attribute.STATE))
        .isEqualTo(TransactionState.COMMITTED.get());

    assertThat(mergedResult.contains(Attribute.VERSION)).isTrue();
    assertThat(mergedResult.isNull(Attribute.VERSION)).isFalse();
    assertThat(mergedResult.getInt(Attribute.VERSION)).isEqualTo(ANY_VERSION_2);
    assertThat(mergedResult.getAsObject(Attribute.VERSION)).isEqualTo(ANY_VERSION_2);

    assertThat(mergedResult.contains(Attribute.BEFORE_PREFIX + ANY_NAME_3)).isTrue();
    assertThat(mergedResult.isNull(Attribute.BEFORE_PREFIX + ANY_NAME_3)).isFalse();
    assertThat(mergedResult.getInt(Attribute.BEFORE_PREFIX + ANY_NAME_3)).isEqualTo(ANY_INT_1);
    assertThat(mergedResult.getAsObject(Attribute.BEFORE_PREFIX + ANY_NAME_3)).isEqualTo(ANY_INT_1);

    assertThat(mergedResult.contains(Attribute.BEFORE_PREFIX + ANY_NAME_4)).isTrue();
    assertThat(mergedResult.isNull(Attribute.BEFORE_PREFIX + ANY_NAME_4)).isFalse();
    assertThat(mergedResult.getText(Attribute.BEFORE_PREFIX + ANY_NAME_4)).isEqualTo(ANY_TEXT_4);
    assertThat(mergedResult.getAsObject(Attribute.BEFORE_PREFIX + ANY_NAME_4))
        .isEqualTo(ANY_TEXT_4);

    assertThat(mergedResult.contains(Attribute.BEFORE_ID)).isTrue();
    assertThat(mergedResult.isNull(Attribute.BEFORE_ID)).isFalse();
    assertThat(mergedResult.getText(Attribute.BEFORE_ID)).isEqualTo(ANY_ID_1);
    assertThat(mergedResult.getAsObject(Attribute.BEFORE_ID)).isEqualTo(ANY_ID_1);

    assertThat(mergedResult.contains(Attribute.BEFORE_PREPARED_AT)).isTrue();
    assertThat(mergedResult.isNull(Attribute.BEFORE_PREPARED_AT)).isFalse();
    assertThat(mergedResult.getBigInt(Attribute.BEFORE_PREPARED_AT)).isEqualTo(ANY_TIME_1);
    assertThat(mergedResult.getAsObject(Attribute.BEFORE_PREPARED_AT)).isEqualTo(ANY_TIME_1);

    assertThat(mergedResult.contains(Attribute.BEFORE_COMMITTED_AT)).isTrue();
    assertThat(mergedResult.isNull(Attribute.BEFORE_COMMITTED_AT)).isFalse();
    assertThat(mergedResult.getBigInt(Attribute.BEFORE_COMMITTED_AT)).isEqualTo(ANY_TIME_2);
    assertThat(mergedResult.getAsObject(Attribute.BEFORE_COMMITTED_AT)).isEqualTo(ANY_TIME_2);

    assertThat(mergedResult.contains(Attribute.BEFORE_STATE)).isTrue();
    assertThat(mergedResult.isNull(Attribute.BEFORE_STATE)).isFalse();
    assertThat(mergedResult.getInt(Attribute.BEFORE_STATE))
        .isEqualTo(TransactionState.COMMITTED.get());
    assertThat(mergedResult.getAsObject(Attribute.BEFORE_STATE))
        .isEqualTo(TransactionState.COMMITTED.get());

    assertThat(mergedResult.contains(Attribute.BEFORE_VERSION)).isTrue();
    assertThat(mergedResult.isNull(Attribute.BEFORE_VERSION)).isFalse();
    assertThat(mergedResult.getInt(Attribute.BEFORE_VERSION)).isEqualTo(ANY_VERSION_1);
    assertThat(mergedResult.getAsObject(Attribute.BEFORE_VERSION)).isEqualTo(ANY_VERSION_1);
  }

  @Test
  public void getValue_OnlyPutGiven_ShouldReturnMergedValue() {
    // Arrange
    Put put =
        new Put(new Key(ANY_NAME_1, ANY_TEXT_1), new Key(ANY_NAME_2, ANY_TEXT_2))
            .withValue(ANY_NAME_3, ANY_INT_3);

    MergedResult mergedResult = new MergedResult(Optional.empty(), put, TABLE_METADATA);

    // Act Assert
    assertThat(mergedResult.getValue(ANY_NAME_1))
        .isEqualTo(Optional.of(new TextValue(ANY_NAME_1, ANY_TEXT_1)));
    assertThat(mergedResult.getValue(ANY_NAME_2))
        .isEqualTo(Optional.of(new TextValue(ANY_NAME_2, ANY_TEXT_2)));
    assertThat(mergedResult.getValue(ANY_NAME_3))
        .isEqualTo(Optional.of(new IntValue(ANY_NAME_3, ANY_INT_3)));
    assertThat(mergedResult.getValue(ANY_NAME_4))
        .isEqualTo(Optional.of(new TextValue(ANY_NAME_4, (String) null)));
    assertThat(mergedResult.getValue(Attribute.ID))
        .isEqualTo(Optional.of(Attribute.toIdValue(null)));
    assertThat(mergedResult.getValue(Attribute.PREPARED_AT))
        .isEqualTo(Optional.of(Attribute.toPreparedAtValue(0L)));
    assertThat(mergedResult.getValue(Attribute.COMMITTED_AT))
        .isEqualTo(Optional.of(Attribute.toCommittedAtValue(0L)));
    assertThat(mergedResult.getValue(Attribute.STATE))
        .isEqualTo(Optional.of(new IntValue(Attribute.STATE, 0)));
    assertThat(mergedResult.getValue(Attribute.VERSION))
        .isEqualTo(Optional.of(Attribute.toVersionValue(0)));
    assertThat(mergedResult.getValue(Attribute.BEFORE_PREFIX + ANY_NAME_3))
        .isEqualTo(Optional.of(new IntValue(Attribute.BEFORE_PREFIX + ANY_NAME_3, 0)));
    assertThat(mergedResult.getValue(Attribute.BEFORE_ID))
        .isEqualTo(Optional.of(Attribute.toBeforeIdValue(null)));
    assertThat(mergedResult.getValue(Attribute.BEFORE_PREPARED_AT))
        .isEqualTo(Optional.of(Attribute.toBeforePreparedAtValue(0L)));
    assertThat(mergedResult.getValue(Attribute.BEFORE_COMMITTED_AT))
        .isEqualTo(Optional.of(Attribute.toBeforeCommittedAtValue(0L)));
    assertThat(mergedResult.getValue(Attribute.BEFORE_STATE))
        .isEqualTo(Optional.of(new IntValue(Attribute.BEFORE_STATE, 0)));
    assertThat(mergedResult.getValue(Attribute.BEFORE_VERSION))
        .isEqualTo(Optional.of(Attribute.toBeforeVersionValue(0)));

    assertThat(mergedResult.getContainedColumnNames())
        .isEqualTo(
            new HashSet<>(
                Arrays.asList(
                    ANY_NAME_1,
                    ANY_NAME_2,
                    ANY_NAME_3,
                    ANY_NAME_4,
                    Attribute.ID,
                    Attribute.PREPARED_AT,
                    Attribute.COMMITTED_AT,
                    Attribute.STATE,
                    Attribute.VERSION,
                    Attribute.BEFORE_PREFIX + ANY_NAME_3,
                    Attribute.BEFORE_PREFIX + ANY_NAME_4,
                    Attribute.BEFORE_ID,
                    Attribute.BEFORE_PREPARED_AT,
                    Attribute.BEFORE_COMMITTED_AT,
                    Attribute.BEFORE_STATE,
                    Attribute.BEFORE_VERSION)));

    assertThat(mergedResult.contains(ANY_NAME_1)).isTrue();
    assertThat(mergedResult.isNull(ANY_NAME_1)).isFalse();
    assertThat(mergedResult.getText(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);
    assertThat(mergedResult.getAsObject(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);

    assertThat(mergedResult.contains(ANY_NAME_2)).isTrue();
    assertThat(mergedResult.isNull(ANY_NAME_2)).isFalse();
    assertThat(mergedResult.getText(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);
    assertThat(mergedResult.getAsObject(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);

    assertThat(mergedResult.contains(ANY_NAME_3)).isTrue();
    assertThat(mergedResult.isNull(ANY_NAME_3)).isFalse();
    assertThat(mergedResult.getInt(ANY_NAME_3)).isEqualTo(ANY_INT_3);
    assertThat(mergedResult.getAsObject(ANY_NAME_3)).isEqualTo(ANY_INT_3);

    assertThat(mergedResult.contains(ANY_NAME_4)).isTrue();
    assertThat(mergedResult.isNull(ANY_NAME_4)).isTrue();
    assertThat(mergedResult.getText(ANY_NAME_4)).isNull();
    assertThat(mergedResult.getAsObject(ANY_NAME_4)).isNull();

    assertThat(mergedResult.contains(Attribute.ID)).isTrue();
    assertThat(mergedResult.isNull(Attribute.ID)).isTrue();
    assertThat(mergedResult.getText(Attribute.ID)).isNull();
    assertThat(mergedResult.getAsObject(Attribute.ID)).isNull();

    assertThat(mergedResult.contains(Attribute.PREPARED_AT)).isTrue();
    assertThat(mergedResult.isNull(Attribute.PREPARED_AT)).isTrue();
    assertThat(mergedResult.getBigInt(Attribute.PREPARED_AT)).isEqualTo(0L);
    assertThat(mergedResult.getAsObject(Attribute.PREPARED_AT)).isNull();

    assertThat(mergedResult.contains(Attribute.COMMITTED_AT)).isTrue();
    assertThat(mergedResult.isNull(Attribute.COMMITTED_AT)).isTrue();
    assertThat(mergedResult.getBigInt(Attribute.COMMITTED_AT)).isEqualTo(0L);
    assertThat(mergedResult.getAsObject(Attribute.COMMITTED_AT)).isNull();

    assertThat(mergedResult.contains(Attribute.STATE)).isTrue();
    assertThat(mergedResult.isNull(Attribute.STATE)).isTrue();
    assertThat(mergedResult.getInt(Attribute.STATE)).isEqualTo(0);
    assertThat(mergedResult.getAsObject(Attribute.STATE)).isNull();

    assertThat(mergedResult.contains(Attribute.VERSION)).isTrue();
    assertThat(mergedResult.isNull(Attribute.VERSION)).isTrue();
    assertThat(mergedResult.getInt(Attribute.VERSION)).isEqualTo(0);
    assertThat(mergedResult.getAsObject(Attribute.VERSION)).isNull();

    assertThat(mergedResult.contains(Attribute.BEFORE_PREFIX + ANY_NAME_3)).isTrue();
    assertThat(mergedResult.isNull(Attribute.BEFORE_PREFIX + ANY_NAME_3)).isTrue();
    assertThat(mergedResult.getInt(Attribute.BEFORE_PREFIX + ANY_NAME_3)).isEqualTo(0);
    assertThat(mergedResult.getAsObject(Attribute.BEFORE_PREFIX + ANY_NAME_3)).isNull();

    assertThat(mergedResult.contains(Attribute.BEFORE_PREFIX + ANY_NAME_4)).isTrue();
    assertThat(mergedResult.isNull(Attribute.BEFORE_PREFIX + ANY_NAME_4)).isTrue();
    assertThat(mergedResult.getText(Attribute.BEFORE_PREFIX + ANY_NAME_4)).isNull();
    assertThat(mergedResult.getAsObject(Attribute.BEFORE_PREFIX + ANY_NAME_4)).isNull();

    assertThat(mergedResult.contains(Attribute.BEFORE_ID)).isTrue();
    assertThat(mergedResult.isNull(Attribute.BEFORE_ID)).isTrue();
    assertThat(mergedResult.getText(Attribute.BEFORE_ID)).isNull();
    assertThat(mergedResult.getAsObject(Attribute.BEFORE_ID)).isNull();

    assertThat(mergedResult.contains(Attribute.BEFORE_PREPARED_AT)).isTrue();
    assertThat(mergedResult.isNull(Attribute.BEFORE_PREPARED_AT)).isTrue();
    assertThat(mergedResult.getBigInt(Attribute.BEFORE_PREPARED_AT)).isEqualTo(0L);
    assertThat(mergedResult.getAsObject(Attribute.BEFORE_PREPARED_AT)).isNull();

    assertThat(mergedResult.contains(Attribute.BEFORE_COMMITTED_AT)).isTrue();
    assertThat(mergedResult.isNull(Attribute.BEFORE_COMMITTED_AT)).isTrue();
    assertThat(mergedResult.getBigInt(Attribute.BEFORE_COMMITTED_AT)).isEqualTo(0L);
    assertThat(mergedResult.getAsObject(Attribute.BEFORE_COMMITTED_AT)).isNull();

    assertThat(mergedResult.contains(Attribute.BEFORE_STATE)).isTrue();
    assertThat(mergedResult.isNull(Attribute.BEFORE_STATE)).isTrue();
    assertThat(mergedResult.getInt(Attribute.BEFORE_STATE)).isEqualTo(0);
    assertThat(mergedResult.getAsObject(Attribute.BEFORE_STATE)).isNull();

    assertThat(mergedResult.contains(Attribute.BEFORE_VERSION)).isTrue();
    assertThat(mergedResult.isNull(Attribute.BEFORE_VERSION)).isTrue();
    assertThat(mergedResult.getInt(Attribute.BEFORE_VERSION)).isEqualTo(0);
    assertThat(mergedResult.getAsObject(Attribute.BEFORE_VERSION)).isNull();
  }

  @Test
  public void getValue_ResultAndPutWithNullValueGiven_ShouldReturnMergedValue() {
    // Arrange
    Put put =
        new Put(new Key(ANY_NAME_1, ANY_TEXT_1), new Key(ANY_NAME_2, ANY_TEXT_2))
            .withValue(ANY_NAME_3, ANY_INT_3)
            .withTextValue(ANY_NAME_4, null);

    MergedResult mergedResult = new MergedResult(Optional.of(result), put, TABLE_METADATA);

    // Act Assert
    assertThat(mergedResult.getValue(ANY_NAME_1))
        .isEqualTo(Optional.of(new TextValue(ANY_NAME_1, ANY_TEXT_1)));
    assertThat(mergedResult.getValue(ANY_NAME_2))
        .isEqualTo(Optional.of(new TextValue(ANY_NAME_2, ANY_TEXT_2)));
    assertThat(mergedResult.getValue(ANY_NAME_3))
        .isEqualTo(Optional.of(new IntValue(ANY_NAME_3, ANY_INT_3)));
    assertThat(mergedResult.getValue(ANY_NAME_4))
        .isEqualTo(Optional.of(new TextValue(ANY_NAME_4, (String) null)));
    assertThat(mergedResult.getValue(Attribute.ID))
        .isEqualTo(Optional.of(Attribute.toIdValue(ANY_ID_2)));
    assertThat(mergedResult.getValue(Attribute.PREPARED_AT))
        .isEqualTo(Optional.of(Attribute.toPreparedAtValue(ANY_TIME_3)));
    assertThat(mergedResult.getValue(Attribute.COMMITTED_AT))
        .isEqualTo(Optional.of(Attribute.toCommittedAtValue(ANY_TIME_4)));
    assertThat(mergedResult.getValue(Attribute.STATE))
        .isEqualTo(Optional.of(Attribute.toStateValue(TransactionState.COMMITTED)));
    assertThat(mergedResult.getValue(Attribute.VERSION))
        .isEqualTo(Optional.of(Attribute.toVersionValue(ANY_VERSION_2)));
    assertThat(mergedResult.getValue(Attribute.BEFORE_PREFIX + ANY_NAME_3))
        .isEqualTo(Optional.of(new IntValue(Attribute.BEFORE_PREFIX + ANY_NAME_3, ANY_INT_1)));
    assertThat(mergedResult.getValue(Attribute.BEFORE_ID))
        .isEqualTo(Optional.of(Attribute.toBeforeIdValue(ANY_ID_1)));
    assertThat(mergedResult.getValue(Attribute.BEFORE_PREPARED_AT))
        .isEqualTo(Optional.of(Attribute.toBeforePreparedAtValue(ANY_TIME_1)));
    assertThat(mergedResult.getValue(Attribute.BEFORE_COMMITTED_AT))
        .isEqualTo(Optional.of(Attribute.toBeforeCommittedAtValue(ANY_TIME_2)));
    assertThat(mergedResult.getValue(Attribute.BEFORE_STATE))
        .isEqualTo(Optional.of(Attribute.toBeforeStateValue(TransactionState.COMMITTED)));
    assertThat(mergedResult.getValue(Attribute.BEFORE_VERSION))
        .isEqualTo(Optional.of(Attribute.toBeforeVersionValue(ANY_VERSION_1)));

    assertThat(mergedResult.getContainedColumnNames())
        .isEqualTo(
            new HashSet<>(
                Arrays.asList(
                    ANY_NAME_1,
                    ANY_NAME_2,
                    ANY_NAME_3,
                    ANY_NAME_4,
                    Attribute.ID,
                    Attribute.PREPARED_AT,
                    Attribute.COMMITTED_AT,
                    Attribute.STATE,
                    Attribute.VERSION,
                    Attribute.BEFORE_PREFIX + ANY_NAME_3,
                    Attribute.BEFORE_PREFIX + ANY_NAME_4,
                    Attribute.BEFORE_ID,
                    Attribute.BEFORE_PREPARED_AT,
                    Attribute.BEFORE_COMMITTED_AT,
                    Attribute.BEFORE_STATE,
                    Attribute.BEFORE_VERSION)));

    assertThat(mergedResult.contains(ANY_NAME_1)).isTrue();
    assertThat(mergedResult.isNull(ANY_NAME_1)).isFalse();
    assertThat(mergedResult.getText(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);
    assertThat(mergedResult.getAsObject(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);

    assertThat(mergedResult.contains(ANY_NAME_2)).isTrue();
    assertThat(mergedResult.isNull(ANY_NAME_2)).isFalse();
    assertThat(mergedResult.getText(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);
    assertThat(mergedResult.getAsObject(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);

    assertThat(mergedResult.contains(ANY_NAME_3)).isTrue();
    assertThat(mergedResult.isNull(ANY_NAME_3)).isFalse();
    assertThat(mergedResult.getInt(ANY_NAME_3)).isEqualTo(ANY_INT_3);
    assertThat(mergedResult.getAsObject(ANY_NAME_3)).isEqualTo(ANY_INT_3);

    assertThat(mergedResult.contains(ANY_NAME_4)).isTrue();
    assertThat(mergedResult.isNull(ANY_NAME_4)).isTrue();
    assertThat(mergedResult.getText(ANY_NAME_4)).isNull();
    assertThat(mergedResult.getAsObject(ANY_NAME_4)).isNull();

    assertThat(mergedResult.contains(Attribute.ID)).isTrue();
    assertThat(mergedResult.isNull(Attribute.ID)).isFalse();
    assertThat(mergedResult.getText(Attribute.ID)).isEqualTo(ANY_ID_2);
    assertThat(mergedResult.getAsObject(Attribute.ID)).isEqualTo(ANY_ID_2);

    assertThat(mergedResult.contains(Attribute.PREPARED_AT)).isTrue();
    assertThat(mergedResult.isNull(Attribute.PREPARED_AT)).isFalse();
    assertThat(mergedResult.getBigInt(Attribute.PREPARED_AT)).isEqualTo(ANY_TIME_3);
    assertThat(mergedResult.getAsObject(Attribute.PREPARED_AT)).isEqualTo(ANY_TIME_3);

    assertThat(mergedResult.contains(Attribute.COMMITTED_AT)).isTrue();
    assertThat(mergedResult.isNull(Attribute.COMMITTED_AT)).isFalse();
    assertThat(mergedResult.getBigInt(Attribute.COMMITTED_AT)).isEqualTo(ANY_TIME_4);
    assertThat(mergedResult.getAsObject(Attribute.COMMITTED_AT)).isEqualTo(ANY_TIME_4);

    assertThat(mergedResult.contains(Attribute.STATE)).isTrue();
    assertThat(mergedResult.isNull(Attribute.STATE)).isFalse();
    assertThat(mergedResult.getInt(Attribute.STATE)).isEqualTo(TransactionState.COMMITTED.get());
    assertThat(mergedResult.getAsObject(Attribute.STATE))
        .isEqualTo(TransactionState.COMMITTED.get());

    assertThat(mergedResult.contains(Attribute.VERSION)).isTrue();
    assertThat(mergedResult.isNull(Attribute.VERSION)).isFalse();
    assertThat(mergedResult.getInt(Attribute.VERSION)).isEqualTo(ANY_VERSION_2);
    assertThat(mergedResult.getAsObject(Attribute.VERSION)).isEqualTo(ANY_VERSION_2);

    assertThat(mergedResult.contains(Attribute.BEFORE_PREFIX + ANY_NAME_3)).isTrue();
    assertThat(mergedResult.isNull(Attribute.BEFORE_PREFIX + ANY_NAME_3)).isFalse();
    assertThat(mergedResult.getInt(Attribute.BEFORE_PREFIX + ANY_NAME_3)).isEqualTo(ANY_INT_1);
    assertThat(mergedResult.getAsObject(Attribute.BEFORE_PREFIX + ANY_NAME_3)).isEqualTo(ANY_INT_1);

    assertThat(mergedResult.contains(Attribute.BEFORE_PREFIX + ANY_NAME_4)).isTrue();
    assertThat(mergedResult.isNull(Attribute.BEFORE_PREFIX + ANY_NAME_4)).isFalse();
    assertThat(mergedResult.getText(Attribute.BEFORE_PREFIX + ANY_NAME_4)).isEqualTo(ANY_TEXT_4);
    assertThat(mergedResult.getAsObject(Attribute.BEFORE_PREFIX + ANY_NAME_4))
        .isEqualTo(ANY_TEXT_4);

    assertThat(mergedResult.contains(Attribute.BEFORE_ID)).isTrue();
    assertThat(mergedResult.isNull(Attribute.BEFORE_ID)).isFalse();
    assertThat(mergedResult.getText(Attribute.BEFORE_ID)).isEqualTo(ANY_ID_1);
    assertThat(mergedResult.getAsObject(Attribute.BEFORE_ID)).isEqualTo(ANY_ID_1);

    assertThat(mergedResult.contains(Attribute.BEFORE_PREPARED_AT)).isTrue();
    assertThat(mergedResult.isNull(Attribute.BEFORE_PREPARED_AT)).isFalse();
    assertThat(mergedResult.getBigInt(Attribute.BEFORE_PREPARED_AT)).isEqualTo(ANY_TIME_1);
    assertThat(mergedResult.getAsObject(Attribute.BEFORE_PREPARED_AT)).isEqualTo(ANY_TIME_1);

    assertThat(mergedResult.contains(Attribute.BEFORE_COMMITTED_AT)).isTrue();
    assertThat(mergedResult.isNull(Attribute.BEFORE_COMMITTED_AT)).isFalse();
    assertThat(mergedResult.getBigInt(Attribute.BEFORE_COMMITTED_AT)).isEqualTo(ANY_TIME_2);
    assertThat(mergedResult.getAsObject(Attribute.BEFORE_COMMITTED_AT)).isEqualTo(ANY_TIME_2);

    assertThat(mergedResult.contains(Attribute.BEFORE_STATE)).isTrue();
    assertThat(mergedResult.isNull(Attribute.BEFORE_STATE)).isFalse();
    assertThat(mergedResult.getInt(Attribute.BEFORE_STATE))
        .isEqualTo(TransactionState.COMMITTED.get());
    assertThat(mergedResult.getAsObject(Attribute.BEFORE_STATE))
        .isEqualTo(TransactionState.COMMITTED.get());

    assertThat(mergedResult.contains(Attribute.BEFORE_VERSION)).isTrue();
    assertThat(mergedResult.isNull(Attribute.BEFORE_VERSION)).isFalse();
    assertThat(mergedResult.getInt(Attribute.BEFORE_VERSION)).isEqualTo(ANY_VERSION_1);
    assertThat(mergedResult.getAsObject(Attribute.BEFORE_VERSION)).isEqualTo(ANY_VERSION_1);
  }

  @Test
  public void getValues_ResultAndPutGiven_ShouldReturnMergedValues() {
    // Arrange
    Put put =
        new Put(new Key(ANY_NAME_1, ANY_TEXT_1), new Key(ANY_NAME_2, ANY_TEXT_2))
            .withValue(ANY_NAME_3, ANY_INT_3);

    MergedResult mergedResult = new MergedResult(Optional.of(result), put, TABLE_METADATA);

    // Act
    Map<String, Value<?>> values = mergedResult.getValues();

    // Assert
    assertThat(values.get(ANY_NAME_1)).isEqualTo(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    assertThat(values.get(ANY_NAME_2)).isEqualTo(new TextValue(ANY_NAME_2, ANY_TEXT_2));
    assertThat(values.get(ANY_NAME_3)).isEqualTo(new IntValue(ANY_NAME_3, ANY_INT_3));
    assertThat(values.get(ANY_NAME_4)).isEqualTo(new TextValue(ANY_NAME_4, ANY_TEXT_3));
    assertThat(values.get(Attribute.ID)).isEqualTo(Attribute.toIdValue(ANY_ID_2));
    assertThat(values.get(Attribute.PREPARED_AT))
        .isEqualTo(Attribute.toPreparedAtValue(ANY_TIME_3));
    assertThat(values.get(Attribute.COMMITTED_AT))
        .isEqualTo(Attribute.toCommittedAtValue(ANY_TIME_4));
    assertThat(values.get(Attribute.STATE))
        .isEqualTo(Attribute.toStateValue(TransactionState.COMMITTED));
    assertThat(values.get(Attribute.VERSION)).isEqualTo(Attribute.toVersionValue(ANY_VERSION_2));
    assertThat(values.get(Attribute.BEFORE_PREFIX + ANY_NAME_3))
        .isEqualTo(new IntValue(Attribute.BEFORE_PREFIX + ANY_NAME_3, ANY_INT_1));
    assertThat(values.get(Attribute.BEFORE_ID)).isEqualTo(Attribute.toBeforeIdValue(ANY_ID_1));
    assertThat(values.get(Attribute.BEFORE_PREPARED_AT))
        .isEqualTo(Attribute.toBeforePreparedAtValue(ANY_TIME_1));
    assertThat(values.get(Attribute.BEFORE_COMMITTED_AT))
        .isEqualTo(Attribute.toBeforeCommittedAtValue(ANY_TIME_2));
    assertThat(values.get(Attribute.BEFORE_STATE))
        .isEqualTo(Attribute.toBeforeStateValue(TransactionState.COMMITTED));
    assertThat(values.get(Attribute.BEFORE_VERSION))
        .isEqualTo(Attribute.toBeforeVersionValue(ANY_VERSION_1));
  }

  @Test
  public void getValues_OnlyPutGiven_ShouldReturnMergedValues() {
    // Arrange
    Put put =
        new Put(new Key(ANY_NAME_1, ANY_TEXT_1), new Key(ANY_NAME_2, ANY_TEXT_2))
            .withValue(ANY_NAME_3, ANY_INT_3);

    MergedResult mergedResult = new MergedResult(Optional.empty(), put, TABLE_METADATA);

    // Act
    Map<String, Value<?>> values = mergedResult.getValues();

    // Assert
    assertThat(values.get(ANY_NAME_1)).isEqualTo(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    assertThat(values.get(ANY_NAME_2)).isEqualTo(new TextValue(ANY_NAME_2, ANY_TEXT_2));
    assertThat(values.get(ANY_NAME_3)).isEqualTo(new IntValue(ANY_NAME_3, ANY_INT_3));
    assertThat(values.get(ANY_NAME_4)).isEqualTo(new TextValue(ANY_NAME_4, (String) null));
    assertThat(values.get(Attribute.ID)).isEqualTo(Attribute.toIdValue(null));
    assertThat(values.get(Attribute.PREPARED_AT)).isEqualTo(Attribute.toPreparedAtValue(0L));
    assertThat(values.get(Attribute.COMMITTED_AT)).isEqualTo(Attribute.toCommittedAtValue(0L));
    assertThat(values.get(Attribute.STATE)).isEqualTo(new IntValue(Attribute.STATE, 0));
    assertThat(values.get(Attribute.VERSION)).isEqualTo(Attribute.toVersionValue(0));
    assertThat(values.get(Attribute.BEFORE_PREFIX + ANY_NAME_3))
        .isEqualTo(new IntValue(Attribute.BEFORE_PREFIX + ANY_NAME_3, 0));
    assertThat(values.get(Attribute.BEFORE_ID)).isEqualTo(Attribute.toBeforeIdValue(null));
    assertThat(values.get(Attribute.BEFORE_PREPARED_AT))
        .isEqualTo(Attribute.toBeforePreparedAtValue(0L));
    assertThat(values.get(Attribute.BEFORE_COMMITTED_AT))
        .isEqualTo(Attribute.toBeforeCommittedAtValue(0L));
    assertThat(values.get(Attribute.BEFORE_STATE))
        .isEqualTo(new IntValue(Attribute.BEFORE_STATE, 0));
    assertThat(values.get(Attribute.BEFORE_VERSION)).isEqualTo(Attribute.toBeforeVersionValue(0));
  }

  @Test
  public void getValues_ResultAndPutWithNullValueGiven_ShouldReturnMergedValues() {
    // Arrange
    Put put =
        new Put(new Key(ANY_NAME_1, ANY_TEXT_1), new Key(ANY_NAME_2, ANY_TEXT_2))
            .withValue(ANY_NAME_3, ANY_INT_3)
            .withTextValue(ANY_NAME_4, null);

    MergedResult mergedResult = new MergedResult(Optional.of(result), put, TABLE_METADATA);

    // Act
    Map<String, Value<?>> values = mergedResult.getValues();

    // Assert
    assertThat(values.get(ANY_NAME_1)).isEqualTo(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    assertThat(values.get(ANY_NAME_2)).isEqualTo(new TextValue(ANY_NAME_2, ANY_TEXT_2));
    assertThat(values.get(ANY_NAME_3)).isEqualTo(new IntValue(ANY_NAME_3, ANY_INT_3));
    assertThat(values.get(ANY_NAME_4)).isEqualTo(new TextValue(ANY_NAME_4, (String) null));
    assertThat(values.get(Attribute.ID)).isEqualTo(Attribute.toIdValue(ANY_ID_2));
    assertThat(values.get(Attribute.PREPARED_AT))
        .isEqualTo(Attribute.toPreparedAtValue(ANY_TIME_3));
    assertThat(values.get(Attribute.COMMITTED_AT))
        .isEqualTo(Attribute.toCommittedAtValue(ANY_TIME_4));
    assertThat(values.get(Attribute.STATE))
        .isEqualTo(Attribute.toStateValue(TransactionState.COMMITTED));
    assertThat(values.get(Attribute.VERSION)).isEqualTo(Attribute.toVersionValue(ANY_VERSION_2));
    assertThat(values.get(Attribute.BEFORE_PREFIX + ANY_NAME_3))
        .isEqualTo(new IntValue(Attribute.BEFORE_PREFIX + ANY_NAME_3, ANY_INT_1));
    assertThat(values.get(Attribute.BEFORE_ID)).isEqualTo(Attribute.toBeforeIdValue(ANY_ID_1));
    assertThat(values.get(Attribute.BEFORE_PREPARED_AT))
        .isEqualTo(Attribute.toBeforePreparedAtValue(ANY_TIME_1));
    assertThat(values.get(Attribute.BEFORE_COMMITTED_AT))
        .isEqualTo(Attribute.toBeforeCommittedAtValue(ANY_TIME_2));
    assertThat(values.get(Attribute.BEFORE_STATE))
        .isEqualTo(Attribute.toBeforeStateValue(TransactionState.COMMITTED));
    assertThat(values.get(Attribute.BEFORE_VERSION))
        .isEqualTo(Attribute.toBeforeVersionValue(ANY_VERSION_1));
  }

  @Test
  public void equals_SamePutAndResultGiven_ShouldReturnTrue() {
    // Arrange
    Put put =
        new Put(new Key(ANY_NAME_1, ANY_TEXT_1), new Key(ANY_NAME_2, ANY_TEXT_2))
            .withValue(ANY_NAME_3, ANY_INT_3);

    MergedResult mergedResult = new MergedResult(Optional.of(result), put, TABLE_METADATA);
    MergedResult anotherMergedResult = new MergedResult(Optional.of(result), put, TABLE_METADATA);

    // Act
    boolean isEqual = mergedResult.equals(anotherMergedResult);

    // Assert
    assertThat(isEqual).isTrue();
  }

  @Test
  public void equals_DifferentPutAndResultGiven_ShouldReturnFalse() {
    // Arrange
    Put put =
        new Put(new Key(ANY_NAME_1, ANY_TEXT_1), new Key(ANY_NAME_2, ANY_TEXT_2))
            .withValue(ANY_NAME_3, ANY_INT_3);

    MergedResult mergedResult = new MergedResult(Optional.of(result), put, TABLE_METADATA);
    MergedResult anotherMergedResult = new MergedResult(Optional.empty(), put, TABLE_METADATA);

    // Act
    boolean isEqual = mergedResult.equals(anotherMergedResult);

    // Assert
    assertThat(isEqual).isFalse();
  }

  @Test
  public void equals_ResultImplWithSamePutAndResultGiven_ShouldReturnTrue() {
    // Arrange
    Put put =
        new Put(new Key(ANY_NAME_1, ANY_TEXT_1), new Key(ANY_NAME_2, ANY_TEXT_2))
            .withValue(ANY_NAME_3, ANY_INT_3);

    MergedResult mergedResult = new MergedResult(Optional.of(result), put, TABLE_METADATA);
    Result another =
        new ResultImpl(
            ImmutableMap.<String, Column<?>>builder()
                .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1))
                .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_2))
                .put(ANY_NAME_3, IntColumn.of(ANY_NAME_3, ANY_INT_3))
                .put(ANY_NAME_4, TextColumn.of(ANY_NAME_4, ANY_TEXT_3))
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
                    Attribute.BEFORE_PREFIX + ANY_NAME_4,
                    TextColumn.of(Attribute.BEFORE_PREFIX + ANY_NAME_4, ANY_TEXT_4))
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

    // Act Assert
    assertThat(mergedResult.equals(another)).isTrue();
  }

  @Test
  public void equals_ResultImplWithDifferentPutAndResultGiven_ShouldReturnFalse() {
    // Arrange
    Put put =
        new Put(new Key(ANY_NAME_1, ANY_TEXT_1), new Key(ANY_NAME_2, ANY_TEXT_2))
            .withValue(ANY_NAME_3, ANY_INT_3);

    MergedResult mergedResult = new MergedResult(Optional.of(result), put, TABLE_METADATA);
    Result another = new ResultImpl(Collections.emptyMap(), TABLE_METADATA);

    // Act Assert
    assertThat(mergedResult.equals(another)).isFalse();
  }
}
