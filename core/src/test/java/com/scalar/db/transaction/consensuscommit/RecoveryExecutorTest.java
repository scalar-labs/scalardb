package com.scalar.db.transaction.consensuscommit;

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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class RecoveryExecutorTest {

  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_ID_1 = "id1";
  private static final String ANY_ID_2 = "id2";
  private static final String ANY_ID_3 = "id3";
  private static final long ANY_TIME_MILLIS_1 = 100;
  private static final long ANY_TIME_MILLIS_2 = 200;
  private static final long ANY_TIME_MILLIS_3 = 300;
  private static final long ANY_TIME_MILLIS_4 = 400;
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
  private static final long ANY_BIGINT_1 = 1000L;
  private static final long ANY_BIGINT_2 = 2000L;
  private static final float ANY_FLOAT_1 = 1.23f;
  private static final float ANY_FLOAT_2 = 4.56f;
  private static final double ANY_DOUBLE_1 = 7.89;
  private static final double ANY_DOUBLE_2 = 0.12;
  private static final byte[] ANY_BLOB_1 = new byte[] {1, 2, 3};
  private static final byte[] ANY_BLOB_2 = new byte[] {4, 5, 6};
  private static final LocalDate ANY_DATE_1 = LocalDate.of(2020, 1, 1);
  private static final LocalDate ANY_DATE_2 = LocalDate.of(2021, 1, 1);
  private static final LocalTime ANY_TIME_1 = LocalTime.of(12, 0, 0);
  private static final LocalTime ANY_TIME_2 = LocalTime.of(13, 0, 0);
  private static final LocalDateTime ANY_TIMESTAMP_1 = LocalDateTime.of(2020, 1, 1, 12, 0, 0);
  private static final LocalDateTime ANY_TIMESTAMP_2 = LocalDateTime.of(2021, 1, 1, 13, 0, 0);
  private static final Instant ANY_TIMESTAMPTZ_1 =
      LocalDateTime.of(2020, 1, 1, 12, 0, 0).toInstant(ZoneOffset.UTC);
  private static final Instant ANY_TIMESTAMPTZ_2 =
      LocalDateTime.of(2021, 1, 1, 13, 0, 0).toInstant(ZoneOffset.UTC);

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

  @Mock private DistributedStorage storage;
  @Mock private Coordinator coordinator;
  @Mock private RecoveryHandler recovery;
  @Mock private TransactionTableMetadataManager tableMetadataManager;
  @Mock private Snapshot.Key snapshotKey;
  @Mock private Selection selection;

  private RecoveryExecutor executor;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    executor = new RecoveryExecutor(storage, coordinator, recovery, tableMetadataManager);

    // Arrange
    when(tableMetadataManager.getTransactionTableMetadata(selection))
        .thenReturn(new TransactionTableMetadata(TABLE_METADATA));

    when(selection.forNamespace()).thenReturn(Optional.of(ANY_NAMESPACE_NAME));
    when(selection.forTable()).thenReturn(Optional.of(ANY_TABLE_NAME));
    when(selection.forFullTableName())
        .thenReturn(Optional.of(ANY_NAMESPACE_NAME + "." + ANY_TABLE_NAME));
  }

  private TransactionResult prepareResult(TransactionState state) {
    return prepareResult(state, ANY_ID_2);
  }

  private TransactionResult prepareResult(TransactionState state, String id) {
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
            .put(ID, TextColumn.of(ID, id))
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

  private TransactionResult prepareResultWithoutBeforeImage(TransactionState state) {
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

  private TransactionResult prepareRolledBackResult() {
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
            .put(ID, TextColumn.of(ID, ANY_ID_1))
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
            .put(BEFORE_VERSION, IntColumn.ofNull(BEFORE_VERSION))
            .build();
    return new TransactionResult(new ResultImpl(columns, TABLE_METADATA));
  }

  private TransactionResult prepareRolledForwardResult() {
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
            .put(COMMITTED_AT, BigIntColumn.of(COMMITTED_AT, ANY_TIME_MILLIS_4))
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
            .put(BEFORE_VERSION, IntColumn.ofNull(BEFORE_VERSION))
            .build();
    return new TransactionResult(new ResultImpl(columns, TABLE_METADATA));
  }

  @Test
  public void
      execute_ReturnLatestResultAndRecoverType_CoordinatorExceptionByCoordinatorState_ShouldThrowCrudException()
          throws CoordinatorException, ExecutionException {
    // Arrange
    TransactionResult transactionResult = mock(TransactionResult.class);
    when(transactionResult.getId()).thenReturn(ANY_ID_1);
    when(coordinator.getState(ANY_ID_1)).thenThrow(new CoordinatorException("error"));

    // Act Assert
    assertThatThrownBy(
            () ->
                executor.execute(
                    snapshotKey,
                    selection,
                    transactionResult,
                    ANY_ID_3,
                    RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER))
        .isInstanceOf(CrudException.class);

    // Verify no recovery attempted
    verify(recovery, never()).tryRecover(any(), any(), any());
  }

  @Test
  public void
      execute_ReturnLatestResultAndRecoverType_TransactionNotExpiredAndNoCoordinatorState_ShouldThrowUncommittedRecordException()
          throws CoordinatorException, ExecutionException {
    // Arrange
    TransactionResult transactionResult = prepareResult(TransactionState.PREPARED);
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.empty());
    when(recovery.isTransactionExpired(transactionResult)).thenReturn(false);

    // Act Assert
    assertThatThrownBy(
            () ->
                executor.execute(
                    snapshotKey,
                    selection,
                    transactionResult,
                    ANY_ID_3,
                    RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER))
        .isInstanceOf(UncommittedRecordException.class);

    // Verify no recovery attempted
    verify(recovery, never()).tryRecover(any(), any(), any());
  }

  @Test
  public void
      execute_ReturnLatestResultAndRecoverType_TransactionExpiredAndNoCoordinatorState_AbortMarkerWritten_ShouldReturnBeforeImageAndRollback()
          throws Exception {
    // Arrange
    TransactionResult transactionResult = prepareResult(TransactionState.PREPARED);
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.empty());
    when(recovery.isTransactionExpired(transactionResult)).thenReturn(true);
    when(storage.get(any(Get.class))).thenReturn(Optional.of(transactionResult));
    when(recovery.tryAbortExpiredTransaction(ANY_ID_2)).thenReturn(true);

    // Act
    RecoveryExecutor.Result result =
        executor.execute(
            snapshotKey,
            selection,
            transactionResult,
            ANY_ID_3,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER);

    // Wait for recovery to complete
    result.recoveryFuture.get();

    // Assert: only after the ABORTED state is written do we return the before-image and roll back
    assertThat(result.recoveredResult).hasValue(prepareRolledBackResult());
    assertThat(result.rolledBack).isTrue();
    verify(recovery).rollbackRecord(selection, transactionResult);
    verify(recovery, never()).tryRecover(any(), any(), any());
  }

  @Test
  public void
      execute_ReturnLatestResultAndRecoverType_TransactionExpiredAndNoCoordinatorState_AbortMarkerWritten_RecordWithoutBeforeImage_ShouldReturnEmptyAndRollback()
          throws Exception {
    // Arrange
    TransactionResult transactionResult =
        prepareResultWithoutBeforeImage(TransactionState.PREPARED);
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.empty());
    when(recovery.isTransactionExpired(transactionResult)).thenReturn(true);
    when(storage.get(any(Get.class))).thenReturn(Optional.of(transactionResult));
    when(recovery.tryAbortExpiredTransaction(ANY_ID_2)).thenReturn(true);

    // Act
    RecoveryExecutor.Result result =
        executor.execute(
            snapshotKey,
            selection,
            transactionResult,
            ANY_ID_3,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER);

    // Wait for recovery to complete
    result.recoveryFuture.get();

    // Assert
    assertThat(result.recoveredResult).isEmpty();
    assertThat(result.rolledBack).isTrue();
    verify(recovery).rollbackRecord(selection, transactionResult);
    verify(recovery, never()).tryRecover(any(), any(), any());
  }

  @Test
  public void
      execute_ReturnLatestResultAndRecoverType_TransactionExpiredAndNoCoordinatorState_AbortMarkerConflictAndWinnerCommitted_ShouldReturnAfterImage()
          throws Exception {
    // Arrange: writing the ABORTED state conflicts because the transaction committed concurrently
    TransactionResult transactionResult = prepareResult(TransactionState.PREPARED);
    Coordinator.State committedState =
        new Coordinator.State(ANY_ID_2, TransactionState.COMMITTED, System.currentTimeMillis());
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.empty(), Optional.of(committedState));
    when(recovery.isTransactionExpired(transactionResult)).thenReturn(true);
    when(storage.get(any(Get.class))).thenReturn(Optional.of(transactionResult));
    when(recovery.tryAbortExpiredTransaction(ANY_ID_2)).thenReturn(false);

    executor = spy(executor);
    doReturn(ANY_TIME_MILLIS_4).when(executor).getCommittedAt();

    // Act
    RecoveryExecutor.Result result =
        executor.execute(
            snapshotKey,
            selection,
            transactionResult,
            ANY_ID_3,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER);
    result.recoveryFuture.get();

    // Assert: the committed after-image is returned -- not the stale before-image
    assertThat(result.recoveredResult).hasValue(prepareRolledForwardResult());
    assertThat(result.rolledBack).isFalse();
    verify(recovery)
        .tryRecover(eq(selection), eq(transactionResult), eq(Optional.of(committedState)));
  }

  @Test
  public void
      execute_ReturnLatestResultAndRecoverType_TransactionExpiredAndNoCoordinatorState_AbortMarkerConflictAndWinnerAborted_ShouldReturnBeforeImage()
          throws Exception {
    // Arrange: writing the ABORTED state conflicts because another actor aborted the transaction
    TransactionResult transactionResult = prepareResult(TransactionState.PREPARED);
    Coordinator.State abortedState =
        new Coordinator.State(ANY_ID_2, TransactionState.ABORTED, System.currentTimeMillis());
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.empty(), Optional.of(abortedState));
    when(recovery.isTransactionExpired(transactionResult)).thenReturn(true);
    when(storage.get(any(Get.class))).thenReturn(Optional.of(transactionResult));
    when(recovery.tryAbortExpiredTransaction(ANY_ID_2)).thenReturn(false);

    // Act
    RecoveryExecutor.Result result =
        executor.execute(
            snapshotKey,
            selection,
            transactionResult,
            ANY_ID_3,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER);
    result.recoveryFuture.get();

    // Assert
    assertThat(result.recoveredResult).hasValue(prepareRolledBackResult());
    assertThat(result.rolledBack).isTrue();
    verify(recovery)
        .tryRecover(eq(selection), eq(transactionResult), eq(Optional.of(abortedState)));
  }

  @Test
  public void
      execute_ReturnLatestResultAndRecoverType_TransactionExpiredAndNoCoordinatorState_TryAbortThrowsCoordinatorException_ShouldThrowCrudException()
          throws Exception {
    // Arrange
    TransactionResult transactionResult = prepareResult(TransactionState.PREPARED);
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.empty());
    when(recovery.isTransactionExpired(transactionResult)).thenReturn(true);
    when(storage.get(any(Get.class))).thenReturn(Optional.of(transactionResult));
    when(recovery.tryAbortExpiredTransaction(ANY_ID_2)).thenThrow(CoordinatorException.class);

    // Act Assert
    assertThatThrownBy(
            () ->
                executor.execute(
                    snapshotKey,
                    selection,
                    transactionResult,
                    ANY_ID_3,
                    RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER))
        .isInstanceOf(CrudException.class);

    // Verify no recovery attempted
    verify(recovery, never()).rollbackRecord(any(), any());
    verify(recovery, never()).tryRecover(any(), any(), any());
  }

  @Test
  public void
      execute_ReturnLatestResultAndRecoverType_TransactionExpiredAndNoCoordinatorState_RecordReReadCommitted_ShouldReturnCommittedValue()
          throws Exception {
    // Arrange: the coordinator state was cleaned up after the transaction committed; re-reading the
    // record shows it is now committed.
    TransactionResult transactionResult = prepareResult(TransactionState.PREPARED);
    TransactionResult committedRecord = prepareResult(TransactionState.COMMITTED);
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.empty());
    when(recovery.isTransactionExpired(transactionResult)).thenReturn(true);
    when(storage.get(any(Get.class))).thenReturn(Optional.of(committedRecord));

    // Act
    RecoveryExecutor.Result result =
        executor.execute(
            snapshotKey,
            selection,
            transactionResult,
            ANY_ID_3,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER);
    result.recoveryFuture.get();

    // Assert: the committed value is returned (not a stale before-image), and nothing is recovered
    assertThat(result.recoveredResult).hasValue(committedRecord);
    assertThat(result.rolledBack).isFalse();
    verify(recovery, never()).tryAbortExpiredTransaction(any());
    verify(recovery, never()).tryRecover(any(), any(), any());
    verify(recovery, never()).rollbackRecord(any(), any());
  }

  @Test
  public void
      execute_ReturnLatestResultAndRecoverType_TransactionExpiredAndNoCoordinatorState_RecordReReadAbsent_ShouldReturnEmpty()
          throws Exception {
    // Arrange: the record is gone (a committed delete or a rolled-back insert).
    TransactionResult transactionResult = prepareResult(TransactionState.DELETED);
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.empty());
    when(recovery.isTransactionExpired(transactionResult)).thenReturn(true);
    when(storage.get(any(Get.class))).thenReturn(Optional.empty());

    // Act
    RecoveryExecutor.Result result =
        executor.execute(
            snapshotKey,
            selection,
            transactionResult,
            ANY_ID_3,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER);
    result.recoveryFuture.get();

    // Assert: a committed delete returns empty, never a stale before-image
    assertThat(result.recoveredResult).isEmpty();
    assertThat(result.rolledBack).isTrue();
    verify(recovery, never()).tryAbortExpiredTransaction(any());
    verify(recovery, never()).tryRecover(any(), any(), any());
    verify(recovery, never()).rollbackRecord(any(), any());
  }

  @Test
  public void
      execute_ReturnLatestResultAndRecoverType_TransactionExpiredAndNoCoordinatorState_AbortMarkerWrittenButRecordCommitted_ShouldReturnCommittedValue()
          throws Exception {
    // Arrange: the pre-abort re-read still sees the record PREPARED by the same writer, so the
    // ABORTED state is written. But the writer actually committed and was cleaned up in the race,
    // so the post-abort re-read sees the record committed. The committed value is returned, not a
    // stale before-image.
    TransactionResult transactionResult = prepareResult(TransactionState.PREPARED);
    TransactionResult committedRecord = prepareResult(TransactionState.COMMITTED);
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.empty());
    when(recovery.isTransactionExpired(transactionResult)).thenReturn(true);
    // First (pre-abort) re-read sees PREPARED; second (post-abort) re-read sees committed.
    when(storage.get(any(Get.class)))
        .thenReturn(Optional.of(transactionResult), Optional.of(committedRecord));
    when(recovery.tryAbortExpiredTransaction(ANY_ID_2)).thenReturn(true);

    // Act
    RecoveryExecutor.Result result =
        executor.execute(
            snapshotKey,
            selection,
            transactionResult,
            ANY_ID_3,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER);
    result.recoveryFuture.get();

    // Assert: the committed value is returned and the record is not rolled back
    assertThat(result.recoveredResult).hasValue(committedRecord);
    assertThat(result.rolledBack).isFalse();
    verify(recovery).tryAbortExpiredTransaction(ANY_ID_2);
    verify(recovery, never()).rollbackRecord(any(), any());
    verify(recovery, never()).tryRecover(any(), any(), any());
  }

  @Test
  public void
      execute_ReturnLatestResultAndRecoverType_TransactionExpiredAndNoCoordinatorState_AbortMarkerWrittenButRecordAbsent_ShouldReturnEmpty()
          throws Exception {
    // Arrange: the pre-abort re-read sees the record PREPARED by the same writer, so the ABORTED
    // state is written. But the writer's delete committed and was cleaned up in the race, so the
    // post-abort re-read finds the record gone. Empty is returned, not a stale before-image.
    TransactionResult transactionResult = prepareResult(TransactionState.PREPARED);
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.empty());
    when(recovery.isTransactionExpired(transactionResult)).thenReturn(true);
    // First (pre-abort) re-read sees PREPARED; second (post-abort) re-read finds the record gone.
    when(storage.get(any(Get.class))).thenReturn(Optional.of(transactionResult), Optional.empty());
    when(recovery.tryAbortExpiredTransaction(ANY_ID_2)).thenReturn(true);

    // Act
    RecoveryExecutor.Result result =
        executor.execute(
            snapshotKey,
            selection,
            transactionResult,
            ANY_ID_3,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER);
    result.recoveryFuture.get();

    // Assert: empty is returned and the record is not rolled back
    assertThat(result.recoveredResult).isEmpty();
    assertThat(result.rolledBack).isTrue();
    verify(recovery).tryAbortExpiredTransaction(ANY_ID_2);
    verify(recovery, never()).rollbackRecord(any(), any());
    verify(recovery, never()).tryRecover(any(), any(), any());
  }

  @Test
  public void
      execute_ReturnLatestResultAndRecoverType_TransactionExpiredAndNoCoordinatorState_AbortMarkerWrittenButRecordRePreparedByDifferentTransaction_ShouldReResolveAgainstIt()
          throws Exception {
    // Arrange: the pre-abort re-read sees the record PREPARED by the same writer, so the ABORTED
    // state is written for it. In the race the writer is rolled back and a DIFFERENT transaction
    // re-prepares the record, so the post-abort re-read sees a different writer. Because an
    // intervening commit may have advanced the latest committed value, the loop must NOT return the
    // original writer's (now stale) before-image -- it must re-resolve against the new writer. The
    // new writer's coordinator state is ABORTED, so the record is resolved (rolled back) for it.
    TransactionResult original = prepareResult(TransactionState.PREPARED);
    TransactionResult rePrepared = prepareResult(TransactionState.PREPARED, ANY_ID_1);
    Coordinator.State abortedState =
        new Coordinator.State(ANY_ID_1, TransactionState.ABORTED, System.currentTimeMillis());
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.empty());
    when(coordinator.getState(ANY_ID_1)).thenReturn(Optional.of(abortedState));
    when(recovery.isTransactionExpired(original)).thenReturn(true);
    // First (pre-abort) re-read sees the same writer PREPARED; second (post-abort) re-read sees a
    // different writer's PREPARED record.
    when(storage.get(any(Get.class))).thenReturn(Optional.of(original), Optional.of(rePrepared));
    when(recovery.tryAbortExpiredTransaction(ANY_ID_2)).thenReturn(true);

    // Act
    RecoveryExecutor.Result result =
        executor.execute(
            snapshotKey,
            selection,
            original,
            ANY_ID_3,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER);
    result.recoveryFuture.get();

    // Assert: resolved against the re-preparing transaction (ANY_ID_1), not by returning the
    // original writer's before-image. The original writer was aborted, but the new writer is the
    // one the record is recovered for.
    assertThat(result.rolledBack).isTrue();
    verify(recovery).tryAbortExpiredTransaction(ANY_ID_2);
    verify(recovery, never()).tryAbortExpiredTransaction(ANY_ID_1);
    verify(coordinator).getState(ANY_ID_1);
    verify(recovery).tryRecover(eq(selection), eq(rePrepared), eq(Optional.of(abortedState)));
    verify(recovery, never()).rollbackRecord(any(), any());
  }

  @Test
  public void
      execute_ReturnLatestResultAndRecoverType_TransactionExpiredAndNoCoordinatorState_AbortKeepsConflicting_ShouldThrowCrudConflictExceptionAfterRetryLimit()
          throws Exception {
    // Arrange: the coordinator state stays absent and every abort attempt conflicts (a concurrent
    // actor keeps racing), so the re-resolution loop spins until MAX_RESOLUTION_ATTEMPTS is reached
    // and a CrudConflictException is thrown.
    TransactionResult transactionResult = prepareResult(TransactionState.PREPARED);
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.empty());
    when(recovery.isTransactionExpired(transactionResult)).thenReturn(true);
    when(storage.get(any(Get.class))).thenReturn(Optional.of(transactionResult));
    when(recovery.tryAbortExpiredTransaction(ANY_ID_2)).thenReturn(false);

    // Act Assert
    assertThatThrownBy(
            () ->
                executor.execute(
                    snapshotKey,
                    selection,
                    transactionResult,
                    ANY_ID_3,
                    RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER))
        .isInstanceOf(CrudConflictException.class);

    // The loop runs exactly MAX_RESOLUTION_ATTEMPTS passes, attempting the abort on each.
    verify(recovery, times(RecoveryExecutor.MAX_RESOLUTION_ATTEMPTS))
        .tryAbortExpiredTransaction(ANY_ID_2);
  }

  @Test
  public void
      execute_ReturnLatestResultAndRecoverType_TransactionExpiredAndNoCoordinatorState_RecordRePreparedByDifferentTransaction_ShouldReResolveAgainstIt()
          throws Exception {
    // Arrange: the original writer's coordinator state is absent and it has expired; the re-read
    // shows a DIFFERENT transaction re-prepared the record. The loop re-resolves against that
    // transaction, whose coordinator state is ABORTED, so the record is rolled back for it.
    TransactionResult original = prepareResult(TransactionState.PREPARED);
    TransactionResult rePrepared = prepareResult(TransactionState.PREPARED, ANY_ID_1);
    Coordinator.State abortedState =
        new Coordinator.State(ANY_ID_1, TransactionState.ABORTED, System.currentTimeMillis());
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.empty());
    when(coordinator.getState(ANY_ID_1)).thenReturn(Optional.of(abortedState));
    when(recovery.isTransactionExpired(original)).thenReturn(true);
    when(storage.get(any(Get.class))).thenReturn(Optional.of(rePrepared));

    // Act
    RecoveryExecutor.Result result =
        executor.execute(
            snapshotKey,
            selection,
            original,
            ANY_ID_3,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER);
    result.recoveryFuture.get();

    // Assert: resolved against the re-preparing transaction (ANY_ID_1), not aborted as the original
    assertThat(result.rolledBack).isTrue();
    verify(coordinator).getState(ANY_ID_2);
    verify(coordinator).getState(ANY_ID_1);
    verify(recovery, never()).tryAbortExpiredTransaction(any());
    verify(recovery).tryRecover(eq(selection), eq(rePrepared), eq(Optional.of(abortedState)));
  }

  @Test
  public void
      execute_ReturnLatestResultAndRecoverType_TransactionExpiredAndNoCoordinatorState_RecordRePreparedByDifferentTransactionNotExpired_ShouldThrowUncommittedRecordException()
          throws Exception {
    // Arrange: the original writer's coordinator state is absent and it has expired; the re-read
    // shows a DIFFERENT, still-in-flight transaction re-prepared the record (its coordinator state
    // is absent AND it has NOT expired). The re-resolution must re-apply the expiry guard to the
    // new
    // writer rather than aborting it or returning the original writer's before-image: a live second
    // writer must not be killed, so the loop throws UncommittedRecordException for it (retry).
    TransactionResult original = prepareResult(TransactionState.PREPARED);
    TransactionResult rePrepared = prepareResult(TransactionState.PREPARED, ANY_ID_1);
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.empty());
    when(coordinator.getState(ANY_ID_1)).thenReturn(Optional.empty());
    when(recovery.isTransactionExpired(original)).thenReturn(true);
    when(recovery.isTransactionExpired(rePrepared)).thenReturn(false);
    when(storage.get(any(Get.class))).thenReturn(Optional.of(rePrepared));

    // Act Assert: the exception is raised for the re-preparing (new) writer, not the original.
    assertThatThrownBy(
            () ->
                executor.execute(
                    snapshotKey,
                    selection,
                    original,
                    ANY_ID_3,
                    RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER))
        .isInstanceOfSatisfying(
            UncommittedRecordException.class,
            e -> assertThat(e.getResults()).containsExactly(rePrepared));

    // The expiry guard was re-applied to the new writer (ANY_ID_1), and the live writer was neither
    // aborted nor rolled back.
    verify(coordinator).getState(ANY_ID_2);
    verify(coordinator).getState(ANY_ID_1);
    verify(recovery, never()).tryAbortExpiredTransaction(any());
    verify(recovery, never()).tryRecover(any(), any(), any());
    verify(recovery, never()).rollbackRecord(any(), any());
  }

  @Test
  public void
      execute_ReturnLatestResultAndRecoverType_TransactionExpiredAndNoCoordinatorState_PreAbortReReadThrowsExecutionException_ShouldThrowCrudException()
          throws Exception {
    // Arrange: the coordinator state is absent and the writer expired, but the pre-abort physical
    // re-read fails with an ExecutionException, which must surface as a CrudException.
    TransactionResult transactionResult = prepareResult(TransactionState.PREPARED);
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.empty());
    when(recovery.isTransactionExpired(transactionResult)).thenReturn(true);
    when(storage.get(any(Get.class))).thenThrow(ExecutionException.class);

    // Act Assert
    assertThatThrownBy(
            () ->
                executor.execute(
                    snapshotKey,
                    selection,
                    transactionResult,
                    ANY_ID_3,
                    RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER))
        .isInstanceOf(CrudException.class);

    // The abort is never attempted because the pre-abort re-read failed first.
    verify(recovery, never()).tryAbortExpiredTransaction(any());
  }

  @Test
  public void
      execute_ReturnLatestResultAndRecoverType_TransactionExpiredAndNoCoordinatorState_PostAbortReReadThrowsExecutionException_ShouldThrowCrudException()
          throws Exception {
    // Arrange: the pre-abort re-read sees the same writer PREPARED, so the ABORTED state is
    // written.
    // The post-abort re-read then fails with an ExecutionException, which must surface as a
    // CrudException.
    TransactionResult transactionResult = prepareResult(TransactionState.PREPARED);
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.empty());
    when(recovery.isTransactionExpired(transactionResult)).thenReturn(true);
    when(storage.get(any(Get.class)))
        .thenReturn(Optional.of(transactionResult))
        .thenThrow(ExecutionException.class);
    when(recovery.tryAbortExpiredTransaction(ANY_ID_2)).thenReturn(true);

    // Act Assert
    assertThatThrownBy(
            () ->
                executor.execute(
                    snapshotKey,
                    selection,
                    transactionResult,
                    ANY_ID_3,
                    RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER))
        .isInstanceOf(CrudException.class);

    verify(recovery).tryAbortExpiredTransaction(ANY_ID_2);
    verify(recovery, never()).rollbackRecord(any(), any());
  }

  @Test
  public void
      execute_ReturnLatestResultAndRecoverType_TransactionExpiredAndNoCoordinatorState_RecordKeepsBeingRePreparedByDifferentTransactions_ShouldThrowCrudConflictExceptionAfterRetryLimit()
          throws Exception {
    // Arrange: the coordinator state stays absent and expired, and every physical re-read shows a
    // DIFFERENT transaction has re-prepared the record (modeled by ping-ponging between two ids),
    // so
    // the loop keeps taking the re-prepare branch until MAX_RESOLUTION_ATTEMPTS is reached and a
    // CrudConflictException is thrown. This exercises the retry limit via the re-prepare branch
    // (the
    // abort-conflict branch is covered by AbortKeepsConflicting above).
    TransactionResult original = prepareResult(TransactionState.PREPARED);
    TransactionResult rePreparedAs1 = prepareResult(TransactionState.PREPARED, ANY_ID_1);
    TransactionResult rePreparedAs2 = prepareResult(TransactionState.PREPARED, ANY_ID_2);
    when(coordinator.getState(ANY_ID_1)).thenReturn(Optional.empty());
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.empty());
    when(recovery.isTransactionExpired(any(TransactionResult.class))).thenReturn(true);
    // current starts as ANY_ID_2; each re-read returns the opposite id so the re-prepare branch is
    // taken on every pass.
    when(storage.get(any(Get.class)))
        .thenReturn(
            Optional.of(rePreparedAs1),
            Optional.of(rePreparedAs2),
            Optional.of(rePreparedAs1),
            Optional.of(rePreparedAs2),
            Optional.of(rePreparedAs1));

    // Act Assert
    assertThatThrownBy(
            () ->
                executor.execute(
                    snapshotKey,
                    selection,
                    original,
                    ANY_ID_3,
                    RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER))
        .isInstanceOf(CrudConflictException.class);

    // The loop runs exactly MAX_RESOLUTION_ATTEMPTS passes, re-reading on each, and never aborts
    // (it
    // always takes the re-prepare branch).
    verify(storage, times(RecoveryExecutor.MAX_RESOLUTION_ATTEMPTS)).get(any(Get.class));
    verify(recovery, never()).tryAbortExpiredTransaction(any());
  }

  @Test
  public void execute_ReturnLatestResultAndRecoverType_CoordinatorStateIsAborted_ShouldRollback()
      throws Exception {
    // Arrange
    TransactionResult transactionResult = prepareResult(TransactionState.PREPARED);
    Coordinator.State abortedState =
        new Coordinator.State(ANY_ID_2, TransactionState.ABORTED, System.currentTimeMillis());
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.of(abortedState));

    // Act
    RecoveryExecutor.Result result =
        executor.execute(
            snapshotKey,
            selection,
            transactionResult,
            ANY_ID_3,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER);

    // Wait for recovery to complete
    result.recoveryFuture.get();

    // Assert
    assertThat(result.recoveredResult).hasValue(prepareRolledBackResult());
    assertThat(result.rolledBack).isTrue();
    verify(recovery)
        .tryRecover(eq(selection), eq(transactionResult), eq(Optional.of(abortedState)));
  }

  @Test
  public void
      execute_ReturnLatestResultAndRecoverType_CoordinatorStateIsAborted_RecordWithoutBeforeImage_ShouldRollback()
          throws Exception {
    // Arrange
    TransactionResult transactionResult =
        prepareResultWithoutBeforeImage(TransactionState.PREPARED);
    Coordinator.State abortedState =
        new Coordinator.State(ANY_ID_2, TransactionState.ABORTED, System.currentTimeMillis());
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.of(abortedState));

    // Act
    RecoveryExecutor.Result result =
        executor.execute(
            snapshotKey,
            selection,
            transactionResult,
            ANY_ID_3,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER);

    // Wait for recovery to complete
    result.recoveryFuture.get();

    // Assert
    assertThat(result.recoveredResult).isEmpty();
    assertThat(result.rolledBack).isTrue();
    verify(recovery)
        .tryRecover(eq(selection), eq(transactionResult), eq(Optional.of(abortedState)));
  }

  @Test
  public void
      execute_ReturnLatestResultAndRecoverType_CoordinatorStateIsCommitted_RecordWithPreparedState_ShouldCommit()
          throws Exception {
    // Arrange
    TransactionResult transactionResult = prepareResult(TransactionState.PREPARED);
    Coordinator.State commitState =
        new Coordinator.State(ANY_ID_2, TransactionState.COMMITTED, System.currentTimeMillis());
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.of(commitState));

    executor = spy(executor);
    doReturn(ANY_TIME_MILLIS_4).when(executor).getCommittedAt();

    // Act
    RecoveryExecutor.Result result =
        executor.execute(
            snapshotKey,
            selection,
            transactionResult,
            ANY_ID_3,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER);

    // Wait for recovery to complete
    result.recoveryFuture.get();

    // Assert
    assertThat(result.recoveredResult).hasValue(prepareRolledForwardResult());
    assertThat(result.rolledBack).isFalse();
    verify(recovery).tryRecover(eq(selection), eq(transactionResult), eq(Optional.of(commitState)));
  }

  @Test
  public void
      execute_ReturnLatestResultAndRecoverType_CoordinatorStateIsCommitted_RecordWithDeletedState_ShouldCommit()
          throws Exception {
    // Arrange
    TransactionResult transactionResult = prepareResult(TransactionState.DELETED);
    Coordinator.State commitState =
        new Coordinator.State(ANY_ID_2, TransactionState.COMMITTED, System.currentTimeMillis());
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.of(commitState));

    // Act
    RecoveryExecutor.Result result =
        executor.execute(
            snapshotKey,
            selection,
            transactionResult,
            ANY_ID_3,
            RecoveryExecutor.RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER);

    // Wait for recovery to complete
    result.recoveryFuture.get();

    // Assert
    assertThat(result.recoveredResult).isNotPresent();
    assertThat(result.rolledBack).isFalse();
    verify(recovery).tryRecover(eq(selection), eq(transactionResult), eq(Optional.of(commitState)));
  }

  @Test
  public void
      execute_ReturnCommittedResultAndRecoverType_CoordinatorExceptionByCoordinatorState_ShouldThrowExecutionExceptionCausedByCrudExceptionByRecoveryFuture()
          throws CoordinatorException, ExecutionException, CrudException {
    // Arrange
    TransactionResult transactionResult = prepareResult(TransactionState.PREPARED);
    when(coordinator.getState(ANY_ID_2)).thenThrow(new CoordinatorException("error"));

    // Act
    RecoveryExecutor.Result result =
        executor.execute(
            snapshotKey,
            selection,
            transactionResult,
            ANY_ID_3,
            RecoveryExecutor.RecoveryType.RETURN_COMMITTED_RESULT_AND_RECOVER);

    // Assert
    assertThatThrownBy(result.recoveryFuture::get)
        .isInstanceOf(java.util.concurrent.ExecutionException.class)
        .hasCauseInstanceOf(CrudException.class);

    assertThat(result.recoveredResult).hasValue(prepareRolledBackResult());
    assertThat(result.rolledBack).isTrue();

    // Verify no recovery attempted
    verify(recovery, never()).tryRecover(any(), any(), any());
  }

  @Test
  public void
      execute_ReturnCommittedResultAndRecoverType_TransactionNotExpiredAndNoCoordinatorState_ShouldThrowExecutionExceptionCauseByUncommittedRecordException()
          throws CoordinatorException, ExecutionException, CrudException {
    // Arrange
    TransactionResult transactionResult = prepareResult(TransactionState.PREPARED);
    when(coordinator.getState(ANY_ID_1)).thenReturn(Optional.empty());
    when(recovery.isTransactionExpired(transactionResult)).thenReturn(false);

    // Act
    RecoveryExecutor.Result result =
        executor.execute(
            snapshotKey,
            selection,
            transactionResult,
            ANY_ID_3,
            RecoveryExecutor.RecoveryType.RETURN_COMMITTED_RESULT_AND_RECOVER);

    // Assert
    assertThatThrownBy(result.recoveryFuture::get)
        .isInstanceOf(java.util.concurrent.ExecutionException.class)
        .hasCauseInstanceOf(UncommittedRecordException.class);

    assertThat(result.recoveredResult).hasValue(prepareRolledBackResult());
    assertThat(result.rolledBack).isTrue();

    // Verify no recovery attempted
    verify(recovery, never()).tryRecover(any(), any(), any());
  }

  @Test
  public void
      execute_ReturnCommittedResultAndRecoverType_TransactionExpiredAndNoCoordinatorState_ShouldRollback()
          throws Exception {
    // Arrange
    TransactionResult transactionResult = prepareResult(TransactionState.PREPARED);
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.empty());
    when(recovery.isTransactionExpired(transactionResult)).thenReturn(true);

    // Act
    RecoveryExecutor.Result result =
        executor.execute(
            snapshotKey,
            selection,
            transactionResult,
            ANY_ID_3,
            RecoveryExecutor.RecoveryType.RETURN_COMMITTED_RESULT_AND_RECOVER);

    // Wait for recovery to complete
    result.recoveryFuture.get();

    // Assert
    assertThat(result.recoveredResult).hasValue(prepareRolledBackResult());
    assertThat(result.rolledBack).isTrue();
    verify(recovery).tryRecover(eq(selection), eq(transactionResult), eq(Optional.empty()));
  }

  @Test
  public void
      execute_ReturnCommittedResultAndRecoverType_TransactionExpiredAndNoCoordinatorState_RecordWithoutBeforeImage_ShouldRollback()
          throws Exception {
    // Arrange
    TransactionResult transactionResult =
        prepareResultWithoutBeforeImage(TransactionState.PREPARED);
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.empty());
    when(recovery.isTransactionExpired(transactionResult)).thenReturn(true);

    // Act
    RecoveryExecutor.Result result =
        executor.execute(
            snapshotKey,
            selection,
            transactionResult,
            ANY_ID_3,
            RecoveryExecutor.RecoveryType.RETURN_COMMITTED_RESULT_AND_RECOVER);

    // Wait for recovery to complete
    result.recoveryFuture.get();

    // Assert
    assertThat(result.recoveredResult).isEmpty();
    assertThat(result.rolledBack).isTrue();
    verify(recovery).tryRecover(eq(selection), eq(transactionResult), eq(Optional.empty()));
  }

  @Test
  public void execute_ReturnCommittedResultAndRecoverType_CoordinatorStateIsAborted_ShouldRollback()
      throws Exception {
    // Arrange
    TransactionResult transactionResult = prepareResult(TransactionState.PREPARED);
    Coordinator.State abortedState =
        new Coordinator.State(ANY_ID_2, TransactionState.ABORTED, System.currentTimeMillis());
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.of(abortedState));

    // Act
    RecoveryExecutor.Result result =
        executor.execute(
            snapshotKey,
            selection,
            transactionResult,
            ANY_ID_3,
            RecoveryExecutor.RecoveryType.RETURN_COMMITTED_RESULT_AND_RECOVER);

    // Wait for recovery to complete
    result.recoveryFuture.get();

    // Assert
    assertThat(result.recoveredResult).hasValue(prepareRolledBackResult());
    assertThat(result.rolledBack).isTrue();
    verify(recovery)
        .tryRecover(eq(selection), eq(transactionResult), eq(Optional.of(abortedState)));
  }

  @Test
  public void
      execute_ReturnCommittedResultAndRecoverType_CoordinatorStateIsAborted_RecordWithoutBeforeImage_ShouldRollback()
          throws Exception {
    // Arrange
    TransactionResult transactionResult =
        prepareResultWithoutBeforeImage(TransactionState.PREPARED);
    Coordinator.State abortedState =
        new Coordinator.State(ANY_ID_2, TransactionState.ABORTED, System.currentTimeMillis());
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.of(abortedState));

    // Act
    RecoveryExecutor.Result result =
        executor.execute(
            snapshotKey,
            selection,
            transactionResult,
            ANY_ID_3,
            RecoveryExecutor.RecoveryType.RETURN_COMMITTED_RESULT_AND_RECOVER);

    // Wait for recovery to complete
    result.recoveryFuture.get();

    // Assert
    assertThat(result.recoveredResult).isEmpty();
    assertThat(result.rolledBack).isTrue();
    verify(recovery)
        .tryRecover(eq(selection), eq(transactionResult), eq(Optional.of(abortedState)));
  }

  @Test
  public void
      execute_ReturnCommittedResultAndRecoverType_CoordinatorStateIsCommitted_RecordWithPreparedState_ShouldCommit()
          throws Exception {
    // Arrange
    TransactionResult transactionResult = prepareResult(TransactionState.PREPARED);
    Coordinator.State commitState =
        new Coordinator.State(ANY_ID_2, TransactionState.COMMITTED, System.currentTimeMillis());
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.of(commitState));

    executor = spy(executor);
    doReturn(ANY_TIME_MILLIS_4).when(executor).getCommittedAt();

    // Act
    RecoveryExecutor.Result result =
        executor.execute(
            snapshotKey,
            selection,
            transactionResult,
            ANY_ID_3,
            RecoveryExecutor.RecoveryType.RETURN_COMMITTED_RESULT_AND_RECOVER);

    // Wait for recovery to complete
    result.recoveryFuture.get();

    // Assert
    assertThat(result.recoveredResult).hasValue(prepareRolledBackResult());
    assertThat(result.rolledBack).isTrue();
    verify(recovery).tryRecover(eq(selection), eq(transactionResult), eq(Optional.of(commitState)));
  }

  @Test
  public void
      execute_ReturnCommittedResultAndRecoverType_CoordinatorStateIsCommitted_RecordWithDeletedState_ShouldCommit()
          throws Exception {
    // Arrange
    TransactionResult transactionResult = prepareResult(TransactionState.DELETED);
    Coordinator.State commitState =
        new Coordinator.State(ANY_ID_2, TransactionState.COMMITTED, System.currentTimeMillis());
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.of(commitState));

    // Act
    RecoveryExecutor.Result result =
        executor.execute(
            snapshotKey,
            selection,
            transactionResult,
            ANY_ID_3,
            RecoveryExecutor.RecoveryType.RETURN_COMMITTED_RESULT_AND_RECOVER);

    // Wait for recovery to complete
    result.recoveryFuture.get();

    // Assert
    assertThat(result.recoveredResult).hasValue(prepareRolledBackResult());
    assertThat(result.rolledBack).isTrue();
    verify(recovery).tryRecover(eq(selection), eq(transactionResult), eq(Optional.of(commitState)));
  }

  @Test
  public void
      execute_ReturnCommittedResultAndNotRecoverType_RecordWithoutBeforeImage_ShouldNotRecover()
          throws Exception {
    // Arrange
    TransactionResult transactionResult =
        prepareResultWithoutBeforeImage(TransactionState.PREPARED);

    // Act
    RecoveryExecutor.Result result =
        executor.execute(
            snapshotKey,
            selection,
            transactionResult,
            ANY_ID_3,
            RecoveryExecutor.RecoveryType.RETURN_COMMITTED_RESULT_AND_NOT_RECOVER);

    // Wait for recovery to complete
    result.recoveryFuture.get();

    // Assert
    assertThat(result.recoveredResult).isEmpty();
    assertThat(result.rolledBack).isTrue();

    // Verify no recovery attempted
    verify(recovery, never()).tryRecover(any(), any(), any());
  }

  @Test
  public void
      execute_ReturnCommittedResultAndNotRecoverType_RecordWithPreparedState_ShouldNotRecover()
          throws Exception {
    // Arrange
    TransactionResult transactionResult = prepareResult(TransactionState.PREPARED);

    // Act
    RecoveryExecutor.Result result =
        executor.execute(
            snapshotKey,
            selection,
            transactionResult,
            ANY_ID_3,
            RecoveryExecutor.RecoveryType.RETURN_COMMITTED_RESULT_AND_NOT_RECOVER);

    // Wait for recovery to complete
    result.recoveryFuture.get();

    // Assert
    assertThat(result.recoveredResult).hasValue(prepareRolledBackResult());
    assertThat(result.rolledBack).isTrue();

    // Verify no recovery attempted
    verify(recovery, never()).tryRecover(any(), any(), any());
  }

  @Test
  public void
      execute_ReturnCommittedResultAndNotRecoverType_RecordWithDeletedState_ShouldNotRecover()
          throws Exception {
    // Arrange
    TransactionResult transactionResult = prepareResult(TransactionState.DELETED);

    // Act
    RecoveryExecutor.Result result =
        executor.execute(
            snapshotKey,
            selection,
            transactionResult,
            ANY_ID_3,
            RecoveryExecutor.RecoveryType.RETURN_COMMITTED_RESULT_AND_NOT_RECOVER);

    // Wait for recovery to complete
    result.recoveryFuture.get();

    // Assert
    assertThat(result.recoveredResult).hasValue(prepareRolledBackResult());
    assertThat(result.rolledBack).isTrue();

    // Verify no recovery attempted
    verify(recovery, never()).tryRecover(any(), any(), any());
  }

  @Test
  public void
      executeSynchronously_WithOptionalPresentState_ShouldDelegateToRecoverAndReturnItsResult()
          throws Exception {
    // Arrange
    TransactionResult transactionResult = prepareResult(TransactionState.PREPARED);
    Optional<Coordinator.State> state =
        Optional.of(
            new Coordinator.State(
                ANY_ID_2, TransactionState.COMMITTED, System.currentTimeMillis()));
    when(recovery.recover(selection, transactionResult, state)).thenReturn(true);

    // Act
    boolean actual = executor.executeSynchronously(selection, transactionResult, state);

    // Assert
    assertThat(actual).isTrue();
    verify(recovery).recover(selection, transactionResult, state);
  }

  @Test
  public void
      executeSynchronously_WithOptionalEmptyState_ShouldDelegateToRecoverAndReturnItsResult()
          throws Exception {
    // Arrange
    TransactionResult transactionResult = prepareResult(TransactionState.PREPARED);
    when(recovery.recover(selection, transactionResult, Optional.empty())).thenReturn(false);

    // Act
    boolean actual = executor.executeSynchronously(selection, transactionResult, Optional.empty());

    // Assert
    assertThat(actual).isFalse();
    verify(recovery).recover(selection, transactionResult, Optional.empty());
  }

  @Test
  public void executeSynchronously_WithNonOptionalPresentState_ShouldDelegateToRecover()
      throws Exception {
    // Arrange
    TransactionResult transactionResult = prepareResult(TransactionState.PREPARED);
    Coordinator.State state =
        new Coordinator.State(ANY_ID_2, TransactionState.COMMITTED, System.currentTimeMillis());

    // Act — the present-state overload is used by finishTransaction and returns nothing.
    executor.executeSynchronously(selection, transactionResult, state);

    // Assert — it delegates to the present-state recover overload (which never touches the
    // coordinator and cannot throw CoordinatorException).
    verify(recovery).recover(selection, transactionResult, state);
  }
}
