package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.api.ConditionalExpression.Operator;
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
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.exception.storage.ExecutionException;
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
  private static final String ANY_TEXT_4 = "text4";
  private static final int ANY_INT_1 = 100;
  private static final int ANY_INT_2 = 200;
  private static final int ANY_INT_3 = 300;
  private static final long ANY_BIGINT_1 = 1000L;
  private static final long ANY_BIGINT_2 = 2000L;
  private static final long ANY_BIGINT_3 = 3000L;
  private static final float ANY_FLOAT_1 = 1.23f;
  private static final float ANY_FLOAT_2 = 4.56f;
  private static final float ANY_FLOAT_3 = 7.89f;
  private static final double ANY_DOUBLE_1 = 7.89;
  private static final double ANY_DOUBLE_2 = 0.12;
  private static final double ANY_DOUBLE_3 = 3.45;
  private static final byte[] ANY_BLOB_1 = new byte[] {1, 2, 3};
  private static final byte[] ANY_BLOB_2 = new byte[] {4, 5, 6};
  private static final byte[] ANY_BLOB_3 = new byte[] {7, 8, 9};
  private static final LocalDate ANY_DATE_1 = LocalDate.of(2020, 1, 1);
  private static final LocalDate ANY_DATE_2 = LocalDate.of(2021, 1, 1);
  private static final LocalDate ANY_DATE_3 = LocalDate.of(2022, 1, 1);
  private static final LocalTime ANY_TIME_1 = LocalTime.of(12, 0, 0);
  private static final LocalTime ANY_TIME_2 = LocalTime.of(13, 0, 0);
  private static final LocalTime ANY_TIME_3 = LocalTime.of(14, 0, 0);
  private static final LocalDateTime ANY_TIMESTAMP_1 = LocalDateTime.of(2020, 1, 1, 12, 0, 0);
  private static final LocalDateTime ANY_TIMESTAMP_2 = LocalDateTime.of(2021, 1, 1, 13, 0, 0);
  private static final LocalDateTime ANY_TIMESTAMP_3 = LocalDateTime.of(2022, 1, 1, 14, 0, 0);
  private static final Instant ANY_TIMESTAMPTZ_1 =
      LocalDateTime.of(2020, 1, 1, 12, 0, 0).toInstant(ZoneOffset.UTC);
  private static final Instant ANY_TIMESTAMPTZ_2 =
      LocalDateTime.of(2021, 1, 1, 13, 0, 0).toInstant(ZoneOffset.UTC);
  private static final Instant ANY_TIMESTAMPTZ_3 =
      LocalDateTime.of(2022, 1, 1, 14, 0, 0).toInstant(ZoneOffset.UTC);

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
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_2);
    return new Get(partitionKey, clusteringKey)
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private Scan prepareScan() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    return new Scan(partitionKey)
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private Put preparePut() {
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
        .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
        .intValue(ANY_NAME_3, ANY_INT_3)
        .booleanValue(ANY_NAME_4, false)
        .bigIntValue(ANY_NAME_5, ANY_BIGINT_3)
        .floatValue(ANY_NAME_6, ANY_FLOAT_3)
        .doubleValue(ANY_NAME_7, ANY_DOUBLE_3)
        .textValue(ANY_NAME_8, ANY_TEXT_4)
        .blobValue(ANY_NAME_9, ANY_BLOB_3)
        .dateValue(ANY_NAME_10, ANY_DATE_3)
        .timeValue(ANY_NAME_11, ANY_TIME_3)
        .timestampValue(ANY_NAME_12, ANY_TIMESTAMP_3)
        .timestampTZValue(ANY_NAME_13, ANY_TIMESTAMPTZ_3)
        .build();
  }

  private TransactionResult prepareResult(TransactionState state) {
    ImmutableMap<String, Column<?>> columns =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1))
            .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_2))
            .put(ANY_NAME_3, IntColumn.of(ANY_NAME_3, ANY_INT_2))
            .put(ANY_NAME_4, BooleanColumn.of(ANY_NAME_4, true))
            .put(ANY_NAME_5, BigIntColumn.of(ANY_NAME_5, ANY_BIGINT_2))
            .put(ANY_NAME_6, FloatColumn.of(ANY_NAME_6, ANY_FLOAT_2))
            .put(ANY_NAME_7, DoubleColumn.of(ANY_NAME_7, ANY_DOUBLE_2))
            .put(ANY_NAME_8, TextColumn.of(ANY_NAME_8, ANY_TEXT_4))
            .put(ANY_NAME_9, BlobColumn.of(ANY_NAME_9, ANY_BLOB_2))
            .put(ANY_NAME_10, DateColumn.of(ANY_NAME_10, ANY_DATE_2))
            .put(ANY_NAME_11, TimeColumn.of(ANY_NAME_11, ANY_TIME_2))
            .put(ANY_NAME_12, TimestampColumn.of(ANY_NAME_12, ANY_TIMESTAMP_2))
            .put(ANY_NAME_13, TimestampTZColumn.of(ANY_NAME_13, ANY_TIMESTAMPTZ_2))
            .put(ID, TextColumn.of(ID, ANY_ID_2))
            .put(PREPARED_AT, BigIntColumn.of(PREPARED_AT, ANY_TIME_MILLIS_3))
            .put(STATE, IntColumn.of(STATE, state.get()))
            .put(VERSION, IntColumn.of(VERSION, 1))
            .put(BEFORE_PREFIX + ANY_NAME_3, IntColumn.of(BEFORE_PREFIX + ANY_NAME_3, ANY_INT_1))
            .put(BEFORE_PREFIX + ANY_NAME_4, BooleanColumn.of(BEFORE_PREFIX + ANY_NAME_4, false))
            .put(
                BEFORE_PREFIX + ANY_NAME_5,
                BigIntColumn.of(BEFORE_PREFIX + ANY_NAME_5, ANY_BIGINT_1))
            .put(
                BEFORE_PREFIX + ANY_NAME_6, FloatColumn.of(BEFORE_PREFIX + ANY_NAME_6, ANY_FLOAT_1))
            .put(
                BEFORE_PREFIX + ANY_NAME_7,
                DoubleColumn.of(BEFORE_PREFIX + ANY_NAME_7, ANY_DOUBLE_1))
            .put(BEFORE_PREFIX + ANY_NAME_8, TextColumn.of(BEFORE_PREFIX + ANY_NAME_8, ANY_TEXT_3))
            .put(BEFORE_PREFIX + ANY_NAME_9, BlobColumn.of(BEFORE_PREFIX + ANY_NAME_9, ANY_BLOB_1))
            .put(
                BEFORE_PREFIX + ANY_NAME_10, DateColumn.of(BEFORE_PREFIX + ANY_NAME_10, ANY_DATE_1))
            .put(
                BEFORE_PREFIX + ANY_NAME_11, TimeColumn.of(BEFORE_PREFIX + ANY_NAME_11, ANY_TIME_1))
            .put(
                BEFORE_PREFIX + ANY_NAME_12,
                TimestampColumn.of(BEFORE_PREFIX + ANY_NAME_12, ANY_TIMESTAMP_1))
            .put(
                BEFORE_PREFIX + ANY_NAME_13,
                TimestampTZColumn.of(BEFORE_PREFIX + ANY_NAME_13, ANY_TIMESTAMPTZ_1))
            .put(BEFORE_ID, TextColumn.of(BEFORE_ID, ANY_ID_1))
            .put(BEFORE_PREPARED_AT, BigIntColumn.of(BEFORE_PREPARED_AT, ANY_TIME_MILLIS_1))
            .put(BEFORE_COMMITTED_AT, BigIntColumn.of(BEFORE_COMMITTED_AT, ANY_TIME_MILLIS_2))
            .put(BEFORE_STATE, IntColumn.of(BEFORE_STATE, TransactionState.COMMITTED.get()))
            .put(BEFORE_VERSION, IntColumn.of(BEFORE_VERSION, 1))
            .build();
    return new TransactionResult(new ResultImpl(columns, TABLE_METADATA));
  }

  private TransactionResult prepareInitialResult(String id, TransactionState state) {
    ImmutableMap.Builder<String, Column<?>> builder =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1))
            .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_2))
            .put(ANY_NAME_3, IntColumn.of(ANY_NAME_3, ANY_INT_1))
            .put(ANY_NAME_4, BooleanColumn.of(ANY_NAME_4, false))
            .put(ANY_NAME_5, BigIntColumn.of(ANY_NAME_5, ANY_BIGINT_1))
            .put(ANY_NAME_6, FloatColumn.of(ANY_NAME_6, ANY_FLOAT_1))
            .put(ANY_NAME_7, DoubleColumn.of(ANY_NAME_7, ANY_DOUBLE_1))
            .put(ANY_NAME_8, TextColumn.of(ANY_NAME_8, ANY_TEXT_3))
            .put(ANY_NAME_9, BlobColumn.of(ANY_NAME_9, ANY_BLOB_1))
            .put(ANY_NAME_10, DateColumn.of(ANY_NAME_10, ANY_DATE_1))
            .put(ANY_NAME_11, TimeColumn.of(ANY_NAME_11, ANY_TIME_1))
            .put(ANY_NAME_12, TimestampColumn.of(ANY_NAME_12, ANY_TIMESTAMP_1))
            .put(ANY_NAME_13, TimestampTZColumn.of(ANY_NAME_13, ANY_TIMESTAMPTZ_1))
            .put(ID, TextColumn.of(ID, id))
            .put(PREPARED_AT, BigIntColumn.of(PREPARED_AT, ANY_TIME_MILLIS_1))
            .put(STATE, IntColumn.of(STATE, state.get()))
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
    if (state.equals(TransactionState.COMMITTED)) {
      builder.put(COMMITTED_AT, BigIntColumn.of(COMMITTED_AT, ANY_TIME_MILLIS_2));
    }
    return new TransactionResult(new ResultImpl(builder.build(), TABLE_METADATA));
  }

  private TransactionResult prepareResultWithNullMetadata(TransactionState state) {
    ImmutableMap<String, Column<?>> columns =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1))
            .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_2))
            .put(ANY_NAME_3, IntColumn.of(ANY_NAME_3, ANY_INT_2))
            .put(ANY_NAME_4, BooleanColumn.of(ANY_NAME_4, true))
            .put(ANY_NAME_5, BigIntColumn.of(ANY_NAME_5, ANY_BIGINT_2))
            .put(ANY_NAME_6, FloatColumn.of(ANY_NAME_6, ANY_FLOAT_2))
            .put(ANY_NAME_7, DoubleColumn.of(ANY_NAME_7, ANY_DOUBLE_2))
            .put(ANY_NAME_8, TextColumn.of(ANY_NAME_8, ANY_TEXT_4))
            .put(ANY_NAME_9, BlobColumn.of(ANY_NAME_9, ANY_BLOB_2))
            .put(ANY_NAME_10, DateColumn.of(ANY_NAME_10, ANY_DATE_2))
            .put(ANY_NAME_11, TimeColumn.of(ANY_NAME_11, ANY_TIME_2))
            .put(ANY_NAME_12, TimestampColumn.of(ANY_NAME_12, ANY_TIMESTAMP_2))
            .put(ANY_NAME_13, TimestampTZColumn.of(ANY_NAME_13, ANY_TIMESTAMPTZ_2))
            .put(ID, TextColumn.of(ID, ANY_ID_2))
            .put(PREPARED_AT, BigIntColumn.of(PREPARED_AT, ANY_TIME_MILLIS_1))
            .put(COMMITTED_AT, BigIntColumn.of(COMMITTED_AT, ANY_TIME_MILLIS_1))
            .put(STATE, IntColumn.of(STATE, state.get()))
            .put(VERSION, IntColumn.of(VERSION, 1))
            .put(BEFORE_PREFIX + ANY_NAME_3, IntColumn.of(BEFORE_PREFIX + ANY_NAME_3, ANY_INT_1))
            .put(BEFORE_PREFIX + ANY_NAME_4, BooleanColumn.of(BEFORE_PREFIX + ANY_NAME_4, false))
            .put(
                BEFORE_PREFIX + ANY_NAME_5,
                BigIntColumn.of(BEFORE_PREFIX + ANY_NAME_5, ANY_BIGINT_1))
            .put(
                BEFORE_PREFIX + ANY_NAME_6, FloatColumn.of(BEFORE_PREFIX + ANY_NAME_6, ANY_FLOAT_1))
            .put(
                BEFORE_PREFIX + ANY_NAME_7,
                DoubleColumn.of(BEFORE_PREFIX + ANY_NAME_7, ANY_DOUBLE_1))
            .put(BEFORE_PREFIX + ANY_NAME_8, TextColumn.of(BEFORE_PREFIX + ANY_NAME_8, ANY_TEXT_3))
            .put(BEFORE_PREFIX + ANY_NAME_9, BlobColumn.of(BEFORE_PREFIX + ANY_NAME_9, ANY_BLOB_1))
            .put(
                BEFORE_PREFIX + ANY_NAME_10, DateColumn.of(BEFORE_PREFIX + ANY_NAME_10, ANY_DATE_1))
            .put(
                BEFORE_PREFIX + ANY_NAME_11, TimeColumn.of(BEFORE_PREFIX + ANY_NAME_11, ANY_TIME_1))
            .put(
                BEFORE_PREFIX + ANY_NAME_12,
                TimestampColumn.of(BEFORE_PREFIX + ANY_NAME_12, ANY_TIMESTAMP_1))
            .put(
                BEFORE_PREFIX + ANY_NAME_13,
                TimestampTZColumn.of(BEFORE_PREFIX + ANY_NAME_13, ANY_TIMESTAMPTZ_1))
            .put(BEFORE_ID, TextColumn.ofNull(BEFORE_ID))
            .put(BEFORE_PREPARED_AT, BigIntColumn.ofNull(BEFORE_PREPARED_AT))
            .put(BEFORE_COMMITTED_AT, BigIntColumn.ofNull(BEFORE_COMMITTED_AT))
            .put(BEFORE_STATE, IntColumn.ofNull(BEFORE_STATE))
            .put(BEFORE_VERSION, IntColumn.of(BEFORE_VERSION, 0))
            .build();
    return new TransactionResult(new ResultImpl(columns, TABLE_METADATA));
  }

  private TransactionResult prepareInitialResultWithNullMetadata() {
    ImmutableMap<String, Column<?>> columns =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, ANY_TEXT_1))
            .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, ANY_TEXT_2))
            .put(ANY_NAME_3, IntColumn.of(ANY_NAME_3, ANY_INT_1))
            .put(ANY_NAME_4, BooleanColumn.of(ANY_NAME_4, false))
            .put(ANY_NAME_5, BigIntColumn.of(ANY_NAME_5, ANY_BIGINT_1))
            .put(ANY_NAME_6, FloatColumn.of(ANY_NAME_6, ANY_FLOAT_1))
            .put(ANY_NAME_7, DoubleColumn.of(ANY_NAME_7, ANY_DOUBLE_1))
            .put(ANY_NAME_8, TextColumn.of(ANY_NAME_8, ANY_TEXT_3))
            .put(ANY_NAME_9, BlobColumn.of(ANY_NAME_9, ANY_BLOB_1))
            .put(ANY_NAME_10, DateColumn.of(ANY_NAME_10, ANY_DATE_1))
            .put(ANY_NAME_11, TimeColumn.of(ANY_NAME_11, ANY_TIME_1))
            .put(ANY_NAME_12, TimestampColumn.of(ANY_NAME_12, ANY_TIMESTAMP_1))
            .put(ANY_NAME_13, TimestampTZColumn.of(ANY_NAME_13, ANY_TIMESTAMPTZ_1))
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
            .clusteringKey(get.getClusteringKey().orElse(null))
            .consistency(Consistency.LINEARIZABLE)
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column(ID).isEqualToText(ANY_ID_2))
                    .and(
                        ConditionBuilder.column(STATE)
                            .isEqualToInt(TransactionState.PREPARED.get()))
                    .build());
    extractAfterColumns(prepareInitialResult(ANY_ID_1, TransactionState.COMMITTED))
        .forEach(builder::value);
    Put expected =
        builder
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
            .build();
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void add_GetAndDeletedResultByThisGiven_ShouldComposePut() throws ExecutionException {
    // Arrange
    TransactionResult result = prepareResult(TransactionState.DELETED);
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
            .clusteringKey(get.getClusteringKey().orElse(null))
            .consistency(Consistency.LINEARIZABLE)
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column(ID).isEqualToText(ANY_ID_2))
                    .and(
                        ConditionBuilder.column(STATE).isEqualToInt(TransactionState.DELETED.get()))
                    .build());
    extractAfterColumns(prepareInitialResult(ANY_ID_1, TransactionState.COMMITTED))
        .forEach(builder::value);
    Put expected =
        builder
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
            .build();
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void add_GetAndPreparedResultWithNullMetadataByThisGiven_ShouldComposePut()
      throws ExecutionException {
    // Arrange
    TransactionResult result = prepareResultWithNullMetadata(TransactionState.PREPARED);
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
    Put expected =
        builder
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
            .build();
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
    PutBuilder.Buildable builder =
        Put.newBuilder()
            .namespace(put.forNamespace().get())
            .table(put.forTable().get())
            .partitionKey(put.getPartitionKey())
            .clusteringKey(put.getClusteringKey().orElse(null))
            .consistency(Consistency.LINEARIZABLE)
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column(ID).isEqualToText(ANY_ID_2))
                    .and(
                        ConditionBuilder.column(STATE)
                            .isEqualToInt(TransactionState.PREPARED.get()))
                    .build());
    extractAfterColumns(prepareInitialResult(ANY_ID_1, TransactionState.COMMITTED))
        .forEach(builder::value);
    Put expected =
        builder
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
            .build();
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
    PutBuilder.Buildable builder =
        Put.newBuilder()
            .namespace(put.forNamespace().get())
            .table(put.forTable().get())
            .partitionKey(put.getPartitionKey())
            .clusteringKey(put.getClusteringKey().orElse(null))
            .consistency(Consistency.LINEARIZABLE)
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column(ID).isEqualToText(ANY_ID_2))
                    .and(
                        ConditionBuilder.column(STATE).isEqualToInt(TransactionState.DELETED.get()))
                    .build());
    extractAfterColumns(prepareInitialResult(ANY_ID_1, TransactionState.COMMITTED))
        .forEach(builder::value);
    Put expected =
        builder
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
            .build();
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
    Scan scan = prepareScan();

    // Act
    composer.add(scan, result);

    // Assert
    Put actual = (Put) composer.get().get(0);
    PutBuilder.Buildable builder =
        Put.newBuilder()
            .namespace(scan.forNamespace().get())
            .table(scan.forTable().get())
            .partitionKey(scan.getPartitionKey())
            .clusteringKey(result.getClusteringKey().orElse(null))
            .consistency(Consistency.LINEARIZABLE)
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column(ID).isEqualToText(ANY_ID_2))
                    .and(
                        ConditionBuilder.column(STATE)
                            .isEqualToInt(TransactionState.PREPARED.get()))
                    .build());
    extractAfterColumns(prepareInitialResult(ANY_ID_1, TransactionState.COMMITTED))
        .forEach(builder::value);
    Put expected =
        builder
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
            .build();
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void add_ScanAndDeletedResultByThisGiven_ShouldComposePut() throws ExecutionException {
    // Arrange
    TransactionResult result = prepareResult(TransactionState.DELETED);
    Scan scan = prepareScan();

    // Act
    composer.add(scan, result);

    // Assert
    Put actual = (Put) composer.get().get(0);
    PutBuilder.Buildable builder =
        Put.newBuilder()
            .namespace(scan.forNamespace().get())
            .table(scan.forTable().get())
            .partitionKey(scan.getPartitionKey())
            .clusteringKey(result.getClusteringKey().orElse(null))
            .consistency(Consistency.LINEARIZABLE)
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column(ID).isEqualToText(ANY_ID_2))
                    .and(
                        ConditionBuilder.column(STATE).isEqualToInt(TransactionState.DELETED.get()))
                    .build());
    extractAfterColumns(prepareInitialResult(ANY_ID_1, TransactionState.COMMITTED))
        .forEach(builder::value);
    Put expected =
        builder
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
            .build();
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void add_ScanAndPreparedResultByThisGivenAndBeforeResultNotGiven_ShouldComposeDelete()
      throws ExecutionException {
    // Arrange
    TransactionResult result = prepareInitialResult(ANY_ID_2, TransactionState.PREPARED);
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
            new ConditionalExpression(ID, ANY_ID_2, Operator.EQ),
            new ConditionalExpression(STATE, TransactionState.PREPARED.get(), Operator.EQ)));
    assertThat(actual).isEqualTo(expected);
  }
}
