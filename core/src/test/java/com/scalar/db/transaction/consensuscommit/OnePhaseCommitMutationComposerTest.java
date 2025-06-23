package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.api.ConditionBuilder.*;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_COMMITTED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_ID;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_PREFIX;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_PREPARED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_STATE;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_VERSION;
import static com.scalar.db.transaction.consensuscommit.Attribute.COMMITTED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.ID;
import static com.scalar.db.transaction.consensuscommit.Attribute.PREPARED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.STATE;
import static com.scalar.db.transaction.consensuscommit.Attribute.VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class OnePhaseCommitMutationComposerTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_ID_1 = "id1";
  private static final String ANY_ID_2 = "id2";
  private static final long ANY_TIME_MILLIS_1 = 100;
  private static final long ANY_TIME_MILLIS_2 = 200;
  private static final long ANY_TIME_MILLIS_3 = 300;
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_NAME_4 = "name4";
  private static final String ANY_NAME_5 = "name5";
  private static final String ANY_NAME_6 = "name6";
  private static final String ANY_NAME_7 = "name7";
  private static final String ANY_NAME_8 = "name8";
  private static final String ANY_NAME_9 = "name9";
  private static final String ANY_NAME_10 = "name10";
  private static final String ANY_NAME_11 = "name11";
  private static final String ANY_NAME_12 = "name12";
  private static final String ANY_NAME_13 = "name13";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final int ANY_INT = 100;
  private static final long ANY_BIGINT = 1000L;
  private static final float ANY_FLOAT = 1.23f;
  private static final double ANY_DOUBLE = 7.89;
  private static final byte[] ANY_BLOB = new byte[] {1, 2, 3};
  private static final LocalDate ANY_DATE = LocalDate.of(2020, 1, 1);
  private static final LocalTime ANY_TIME = LocalTime.of(12, 0, 0);
  private static final LocalDateTime ANY_TIMESTAMP = LocalDateTime.of(2020, 1, 1, 12, 0, 0);
  private static final Instant ANY_TIMESTAMPTZ =
      LocalDateTime.of(2020, 1, 1, 12, 0, 0).toInstant(ZoneOffset.UTC);

  private static final TableMetadata TABLE_METADATA =
      ConsensusCommitUtils.buildTransactionTableMetadata(
          TableMetadata.newBuilder()
              .addColumn(ANY_NAME_1, DataType.TEXT)
              .addColumn(ANY_NAME_2, DataType.TEXT)
              .addColumn(ANY_NAME_3, DataType.INT)
              .addColumn(ANY_NAME_4, DataType.BOOLEAN)
              .addColumn(ANY_NAME_5, DataType.BIGINT)
              .addColumn(ANY_NAME_6, DataType.FLOAT)
              .addColumn(ANY_NAME_7, DataType.DOUBLE)
              .addColumn(ANY_NAME_8, DataType.TEXT)
              .addColumn(ANY_NAME_9, DataType.BLOB)
              .addColumn(ANY_NAME_10, DataType.DATE)
              .addColumn(ANY_NAME_11, DataType.TIME)
              .addColumn(ANY_NAME_12, DataType.TIMESTAMP)
              .addColumn(ANY_NAME_13, DataType.TIMESTAMPTZ)
              .addPartitionKey(ANY_NAME_1)
              .addClusteringKey(ANY_NAME_2)
              .build());

  @Mock private TransactionTableMetadataManager tableMetadataManager;

  private OnePhaseCommitMutationComposer composer;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    composer =
        new OnePhaseCommitMutationComposer(ANY_ID_2, ANY_TIME_MILLIS_3, tableMetadataManager);

    when(tableMetadataManager.getTransactionTableMetadata(any(Operation.class)))
        .thenReturn(new TransactionTableMetadata(TABLE_METADATA));
  }

  private Put preparePut() {
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
        .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
        .intValue(ANY_NAME_3, ANY_INT)
        .build();
  }

  private Put preparePutWithInsertMode() {
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
        .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
        .intValue(ANY_NAME_3, ANY_INT)
        .enableInsertMode()
        .build();
  }

  private Delete prepareDelete() {
    return Delete.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
        .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
        .build();
  }

  private TransactionResult prepareInitialResult(String id) {
    ImmutableMap.Builder<String, Column<?>> builder =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1))
            .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_2))
            .put(ANY_NAME_3, IntColumn.of(ANY_NAME_3, ANY_INT))
            .put(ANY_NAME_4, BooleanColumn.of(ANY_NAME_4, false))
            .put(ANY_NAME_5, BigIntColumn.of(ANY_NAME_5, ANY_BIGINT))
            .put(ANY_NAME_6, FloatColumn.of(ANY_NAME_6, ANY_FLOAT))
            .put(ANY_NAME_7, DoubleColumn.of(ANY_NAME_7, ANY_DOUBLE))
            .put(ANY_NAME_8, TextColumn.of(ANY_NAME_8, ANY_TEXT_3))
            .put(ANY_NAME_9, BlobColumn.of(ANY_NAME_9, ANY_BLOB))
            .put(ANY_NAME_10, DateColumn.of(ANY_NAME_10, ANY_DATE))
            .put(ANY_NAME_11, TimeColumn.of(ANY_NAME_11, ANY_TIME))
            .put(ANY_NAME_12, TimestampColumn.of(ANY_NAME_12, ANY_TIMESTAMP))
            .put(ANY_NAME_13, TimestampTZColumn.of(ANY_NAME_13, ANY_TIMESTAMPTZ))
            .put(ID, TextColumn.of(ID, id))
            .put(PREPARED_AT, BigIntColumn.of(PREPARED_AT, ANY_TIME_MILLIS_1))
            .put(COMMITTED_AT, BigIntColumn.of(COMMITTED_AT, ANY_TIME_MILLIS_2))
            .put(STATE, IntColumn.of(STATE, TransactionState.COMMITTED.get()))
            .put(VERSION, IntColumn.of(VERSION, 1))
            .put(BEFORE_PREFIX + ANY_NAME_3, IntColumn.ofNull(BEFORE_PREFIX + ANY_NAME_3))
            .put(BEFORE_PREFIX + ANY_NAME_4, BooleanColumn.ofNull(BEFORE_PREFIX + ANY_NAME_4))
            .put(BEFORE_PREFIX + ANY_NAME_5, BigIntColumn.ofNull(BEFORE_PREFIX + ANY_NAME_5))
            .put(BEFORE_PREFIX + ANY_NAME_6, FloatColumn.ofNull(BEFORE_PREFIX + ANY_NAME_6))
            .put(BEFORE_PREFIX + ANY_NAME_7, DoubleColumn.ofNull(BEFORE_PREFIX + ANY_NAME_7))
            .put(BEFORE_PREFIX + ANY_NAME_8, TextColumn.ofNull(BEFORE_PREFIX + ANY_NAME_8))
            .put(BEFORE_PREFIX + ANY_NAME_9, BlobColumn.ofNull(BEFORE_PREFIX + ANY_NAME_9))
            .put(BEFORE_PREFIX + ANY_NAME_10, DateColumn.ofNull(BEFORE_PREFIX + ANY_NAME_10))
            .put(BEFORE_PREFIX + ANY_NAME_11, TimeColumn.ofNull(BEFORE_PREFIX + ANY_NAME_11))
            .put(BEFORE_PREFIX + ANY_NAME_12, TimestampColumn.ofNull(BEFORE_PREFIX + ANY_NAME_12))
            .put(BEFORE_PREFIX + ANY_NAME_13, TimestampTZColumn.ofNull(BEFORE_PREFIX + ANY_NAME_13))
            .put(BEFORE_ID, TextColumn.ofNull(BEFORE_ID))
            .put(BEFORE_PREPARED_AT, BigIntColumn.ofNull(BEFORE_PREPARED_AT))
            .put(BEFORE_COMMITTED_AT, BigIntColumn.ofNull(BEFORE_COMMITTED_AT))
            .put(BEFORE_STATE, IntColumn.ofNull(BEFORE_STATE))
            .put(BEFORE_VERSION, IntColumn.ofNull(BEFORE_VERSION));
    return new TransactionResult(new ResultImpl(builder.build(), TABLE_METADATA));
  }

  private TransactionResult prepareInitialResultWithNullMetadata() {
    ImmutableMap<String, Column<?>> columns =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1))
            .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_2))
            .put(ANY_NAME_3, IntColumn.of(ANY_NAME_3, ANY_INT))
            .put(ANY_NAME_4, BooleanColumn.of(ANY_NAME_4, false))
            .put(ANY_NAME_5, BigIntColumn.of(ANY_NAME_5, ANY_BIGINT))
            .put(ANY_NAME_6, FloatColumn.of(ANY_NAME_6, ANY_FLOAT))
            .put(ANY_NAME_7, DoubleColumn.of(ANY_NAME_7, ANY_DOUBLE))
            .put(ANY_NAME_8, TextColumn.of(ANY_NAME_8, ANY_TEXT_3))
            .put(ANY_NAME_9, BlobColumn.of(ANY_NAME_9, ANY_BLOB))
            .put(ANY_NAME_10, DateColumn.of(ANY_NAME_10, ANY_DATE))
            .put(ANY_NAME_11, TimeColumn.of(ANY_NAME_11, ANY_TIME))
            .put(ANY_NAME_12, TimestampColumn.of(ANY_NAME_12, ANY_TIMESTAMP))
            .put(ANY_NAME_13, TimestampTZColumn.of(ANY_NAME_13, ANY_TIMESTAMPTZ))
            .put(ID, TextColumn.ofNull(ID))
            .put(PREPARED_AT, BigIntColumn.ofNull(PREPARED_AT))
            .put(COMMITTED_AT, BigIntColumn.ofNull(COMMITTED_AT))
            .put(STATE, IntColumn.ofNull(STATE))
            .put(VERSION, IntColumn.ofNull(VERSION))
            .put(BEFORE_PREFIX + ANY_NAME_3, IntColumn.ofNull(BEFORE_PREFIX + ANY_NAME_3))
            .put(BEFORE_PREFIX + ANY_NAME_4, BooleanColumn.ofNull(BEFORE_PREFIX + ANY_NAME_4))
            .put(BEFORE_PREFIX + ANY_NAME_5, BigIntColumn.ofNull(BEFORE_PREFIX + ANY_NAME_5))
            .put(BEFORE_PREFIX + ANY_NAME_6, FloatColumn.ofNull(BEFORE_PREFIX + ANY_NAME_6))
            .put(BEFORE_PREFIX + ANY_NAME_7, DoubleColumn.ofNull(BEFORE_PREFIX + ANY_NAME_7))
            .put(BEFORE_PREFIX + ANY_NAME_8, TextColumn.ofNull(BEFORE_PREFIX + ANY_NAME_8))
            .put(BEFORE_PREFIX + ANY_NAME_9, BlobColumn.ofNull(BEFORE_PREFIX + ANY_NAME_9))
            .put(BEFORE_PREFIX + ANY_NAME_10, DateColumn.ofNull(BEFORE_PREFIX + ANY_NAME_10))
            .put(BEFORE_PREFIX + ANY_NAME_11, TimeColumn.ofNull(BEFORE_PREFIX + ANY_NAME_11))
            .put(BEFORE_PREFIX + ANY_NAME_12, TimestampColumn.ofNull(BEFORE_PREFIX + ANY_NAME_12))
            .put(BEFORE_PREFIX + ANY_NAME_13, TimestampTZColumn.ofNull(BEFORE_PREFIX + ANY_NAME_13))
            .put(BEFORE_ID, TextColumn.ofNull(BEFORE_ID))
            .put(BEFORE_PREPARED_AT, BigIntColumn.ofNull(BEFORE_PREPARED_AT))
            .put(BEFORE_COMMITTED_AT, BigIntColumn.ofNull(BEFORE_COMMITTED_AT))
            .put(BEFORE_STATE, IntColumn.ofNull(BEFORE_STATE))
            .put(BEFORE_VERSION, IntColumn.ofNull(BEFORE_VERSION))
            .build();
    return new TransactionResult(new ResultImpl(columns, TABLE_METADATA));
  }

  @Test
  public void add_PutAndNullResultGiven_ShouldComposeCorrectly() throws Exception {
    // Arrange
    Put put = preparePut();

    // Act
    composer.add(put, null);

    // Assert
    List<Mutation> mutations = composer.get();
    assertThat(mutations).hasSize(1);
    assertThat(mutations.get(0))
        .isEqualTo(
            Put.newBuilder()
                .namespace(ANY_NAMESPACE_NAME)
                .table(ANY_TABLE_NAME)
                .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
                .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
                .intValue(ANY_NAME_3, ANY_INT)
                .textValue(ID, ANY_ID_2)
                .intValue(STATE, TransactionState.COMMITTED.get())
                .bigIntValue(PREPARED_AT, ANY_TIME_MILLIS_3)
                .bigIntValue(COMMITTED_AT, ANY_TIME_MILLIS_3)
                .intValue(VERSION, 1)
                .condition(putIfNotExists())
                .consistency(Consistency.LINEARIZABLE)
                .build());
  }

  @Test
  public void add_PutWithInsertModeAndNullResultGiven_ShouldComposeCorrectly() throws Exception {
    // Arrange
    Put putWithInsertMode = preparePutWithInsertMode();

    // Act
    composer.add(putWithInsertMode, null);

    // Assert
    List<Mutation> mutations = composer.get();
    assertThat(mutations).hasSize(1);
    assertThat(mutations.get(0))
        .isEqualTo(
            Put.newBuilder()
                .namespace(ANY_NAMESPACE_NAME)
                .table(ANY_TABLE_NAME)
                .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
                .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
                .intValue(ANY_NAME_3, ANY_INT)
                .textValue(ID, ANY_ID_2)
                .intValue(STATE, TransactionState.COMMITTED.get())
                .bigIntValue(PREPARED_AT, ANY_TIME_MILLIS_3)
                .bigIntValue(COMMITTED_AT, ANY_TIME_MILLIS_3)
                .intValue(VERSION, 1)
                .condition(putIfNotExists())
                .consistency(Consistency.LINEARIZABLE)
                .build());
  }

  @Test
  public void add_PutAndExistingCommittedResult_ShouldComposeCorrectly() throws Exception {
    // Arrange
    Put put = preparePut();
    TransactionResult result = prepareInitialResult(ANY_ID_1);

    // Act
    composer.add(put, result);

    // Assert
    List<Mutation> mutations = composer.get();
    assertThat(mutations).hasSize(1);
    assertThat(mutations.get(0))
        .isEqualTo(
            Put.newBuilder()
                .namespace(ANY_NAMESPACE_NAME)
                .table(ANY_TABLE_NAME)
                .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
                .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
                .intValue(ANY_NAME_3, ANY_INT)
                .textValue(ID, ANY_ID_2)
                .intValue(STATE, TransactionState.COMMITTED.get())
                .bigIntValue(PREPARED_AT, ANY_TIME_MILLIS_3)
                .bigIntValue(COMMITTED_AT, ANY_TIME_MILLIS_3)
                .intValue(VERSION, 2) // Incremented version
                .textValue(BEFORE_ID, null)
                .intValue(BEFORE_STATE, null)
                .intValue(BEFORE_VERSION, null)
                .bigIntValue(BEFORE_PREPARED_AT, null)
                .bigIntValue(BEFORE_COMMITTED_AT, null)
                .intValue(BEFORE_PREFIX + ANY_NAME_3, null)
                .booleanValue(BEFORE_PREFIX + ANY_NAME_4, null)
                .bigIntValue(BEFORE_PREFIX + ANY_NAME_5, null)
                .floatValue(BEFORE_PREFIX + ANY_NAME_6, null)
                .doubleValue(BEFORE_PREFIX + ANY_NAME_7, null)
                .textValue(BEFORE_PREFIX + ANY_NAME_8, null)
                .blobValue(BEFORE_PREFIX + ANY_NAME_9, (byte[]) null)
                .dateValue(BEFORE_PREFIX + ANY_NAME_10, null)
                .timeValue(BEFORE_PREFIX + ANY_NAME_11, null)
                .timestampValue(BEFORE_PREFIX + ANY_NAME_12, null)
                .timestampTZValue(BEFORE_PREFIX + ANY_NAME_13, null)
                .condition(putIf(column(ID).isEqualToText(ANY_ID_1)).build())
                .consistency(Consistency.LINEARIZABLE)
                .build());
  }

  @Test
  public void add_PutWithInsertModeAndExistingCommittedResult_ShouldComposeCorrectly()
      throws Exception {
    // Arrange
    Put putWithInsertMode = preparePutWithInsertMode();
    TransactionResult result = prepareInitialResult(ANY_ID_1);

    // Act
    composer.add(putWithInsertMode, result);

    // Assert
    List<Mutation> mutations = composer.get();
    assertThat(mutations).hasSize(1);
    assertThat(mutations.get(0))
        .isEqualTo(
            Put.newBuilder()
                .namespace(ANY_NAMESPACE_NAME)
                .table(ANY_TABLE_NAME)
                .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
                .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
                .intValue(ANY_NAME_3, ANY_INT)
                .textValue(ID, ANY_ID_2)
                .intValue(STATE, TransactionState.COMMITTED.get())
                .bigIntValue(PREPARED_AT, ANY_TIME_MILLIS_3)
                .bigIntValue(COMMITTED_AT, ANY_TIME_MILLIS_3)
                .intValue(VERSION, 1)
                .condition(putIfNotExists())
                .consistency(Consistency.LINEARIZABLE)
                .build());
  }

  @Test
  public void add_PutAndDeemedCommittedResult_ShouldComposeCorrectly() throws Exception {
    // Arrange
    Put put = preparePut();
    TransactionResult resultWithNullMetadata = prepareInitialResultWithNullMetadata();

    // Act
    composer.add(put, resultWithNullMetadata);

    // Assert
    List<Mutation> mutations = composer.get();
    assertThat(mutations).hasSize(1);
    assertThat(mutations.get(0))
        .isEqualTo(
            Put.newBuilder()
                .namespace(ANY_NAMESPACE_NAME)
                .table(ANY_TABLE_NAME)
                .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
                .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
                .intValue(ANY_NAME_3, ANY_INT)
                .textValue(ID, ANY_ID_2)
                .intValue(STATE, TransactionState.COMMITTED.get())
                .bigIntValue(PREPARED_AT, ANY_TIME_MILLIS_3)
                .bigIntValue(COMMITTED_AT, ANY_TIME_MILLIS_3)
                .intValue(VERSION, 1) // The first version
                .textValue(BEFORE_ID, null)
                .intValue(BEFORE_STATE, null)
                .intValue(BEFORE_VERSION, null)
                .bigIntValue(BEFORE_PREPARED_AT, null)
                .bigIntValue(BEFORE_COMMITTED_AT, null)
                .intValue(BEFORE_PREFIX + ANY_NAME_3, null)
                .booleanValue(BEFORE_PREFIX + ANY_NAME_4, null)
                .bigIntValue(BEFORE_PREFIX + ANY_NAME_5, null)
                .floatValue(BEFORE_PREFIX + ANY_NAME_6, null)
                .doubleValue(BEFORE_PREFIX + ANY_NAME_7, null)
                .textValue(BEFORE_PREFIX + ANY_NAME_8, null)
                .blobValue(BEFORE_PREFIX + ANY_NAME_9, (byte[]) null)
                .dateValue(BEFORE_PREFIX + ANY_NAME_10, null)
                .timeValue(BEFORE_PREFIX + ANY_NAME_11, null)
                .timestampValue(BEFORE_PREFIX + ANY_NAME_12, null)
                .timestampTZValue(BEFORE_PREFIX + ANY_NAME_13, null)
                .condition(putIf(column(ID).isNullText()).build())
                .consistency(Consistency.LINEARIZABLE)
                .build());
  }

  @Test
  public void add_DeleteAndExistingCommittedResult_ShouldComposeCorrectly() throws Exception {
    // Arrange
    Delete delete = prepareDelete();
    TransactionResult result = prepareInitialResult(ANY_ID_1);

    // Act
    composer.add(delete, result);

    // Assert
    List<Mutation> mutations = composer.get();
    assertThat(mutations).hasSize(1);
    assertThat(mutations.get(0))
        .isEqualTo(
            Delete.newBuilder()
                .namespace(ANY_NAMESPACE_NAME)
                .table(ANY_TABLE_NAME)
                .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
                .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
                .condition(deleteIf(column(ID).isEqualToText(ANY_ID_1)).build())
                .consistency(Consistency.LINEARIZABLE)
                .build());
  }
}
