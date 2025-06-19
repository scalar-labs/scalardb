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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.exception.storage.ExecutionException;
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

  @Mock private Coordinator coordinator;
  @Mock private RecoveryHandler recovery;
  @Mock private TransactionTableMetadataManager tableMetadataManager;
  @Mock private Snapshot.Key snapshotKey;
  @Mock private Selection selection;

  private RecoveryExecutor executor;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    executor = new RecoveryExecutor(coordinator, recovery, tableMetadataManager);

    // Arrange
    when(tableMetadataManager.getTransactionTableMetadata(selection))
        .thenReturn(new TransactionTableMetadata(TABLE_METADATA));

    when(selection.forNamespace()).thenReturn(Optional.of(ANY_NAMESPACE_NAME));
    when(selection.forTable()).thenReturn(Optional.of(ANY_TABLE_NAME));
    when(selection.forFullTableName())
        .thenReturn(Optional.of(ANY_NAMESPACE_NAME + "." + ANY_TABLE_NAME));
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
  public void execute_CoordinatorExceptionByCoordinatorState_ShouldThrowCrudException()
      throws CoordinatorException, ExecutionException {
    // Arrange
    TransactionResult transactionResult = mock(TransactionResult.class);
    when(transactionResult.getId()).thenReturn(ANY_ID_1);
    when(coordinator.getState(ANY_ID_1)).thenThrow(new CoordinatorException("error"));

    // Act Assert
    assertThatThrownBy(() -> executor.execute(snapshotKey, selection, transactionResult, ANY_ID_3))
        .isInstanceOf(CrudException.class);

    // Verify no recovery attempted
    verify(recovery, never()).recover(any(), any(), any());
  }

  @Test
  public void
      execute_TransactionNotExpiredAndNoCoordinatorState_ShouldThrowUncommittedRecordException()
          throws CoordinatorException, ExecutionException {
    // Arrange
    TransactionResult transactionResult = mock(TransactionResult.class);
    when(transactionResult.getId()).thenReturn(ANY_ID_1);
    when(coordinator.getState(ANY_ID_1)).thenReturn(Optional.empty());
    when(recovery.isTransactionExpired(transactionResult)).thenReturn(false);

    // Act Assert
    assertThatThrownBy(() -> executor.execute(snapshotKey, selection, transactionResult, ANY_ID_3))
        .isInstanceOf(UncommittedRecordException.class);

    // Verify no recovery attempted
    verify(recovery, never()).recover(any(), any(), any());
  }

  @Test
  public void execute_TransactionExpiredAndNoCoordinatorState_ShouldRollback() throws Exception {
    // Arrange
    TransactionResult transactionResult = prepareResult(TransactionState.PREPARED);
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.empty());
    when(recovery.isTransactionExpired(transactionResult)).thenReturn(true);

    // Act
    RecoveryExecutor.Result result =
        executor.execute(snapshotKey, selection, transactionResult, ANY_ID_3);

    // Wait for recovery to complete
    result.recoveryFuture.get();

    // Assert
    assertThat(result.recoveredResult).hasValue(prepareRolledBackResult());
    verify(recovery).recover(eq(selection), eq(transactionResult), eq(Optional.empty()));
  }

  @Test
  public void
      execute_TransactionExpiredAndNoCoordinatorState_RecordWithoutBeforeImage_ShouldRollback()
          throws Exception {
    // Arrange
    TransactionResult transactionResult =
        prepareResultWithoutBeforeImage(TransactionState.PREPARED);
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.empty());
    when(recovery.isTransactionExpired(transactionResult)).thenReturn(true);

    // Act
    RecoveryExecutor.Result result =
        executor.execute(snapshotKey, selection, transactionResult, ANY_ID_3);

    // Wait for recovery to complete
    result.recoveryFuture.get();

    // Assert
    assertThat(result.recoveredResult).isEmpty();
    verify(recovery).recover(eq(selection), eq(transactionResult), eq(Optional.empty()));
  }

  @Test
  public void execute_CoordinatorStateIsAborted_ShouldRollback() throws Exception {
    // Arrange
    TransactionResult transactionResult = prepareResult(TransactionState.PREPARED);
    Coordinator.State abortedState = new Coordinator.State(ANY_ID_2, TransactionState.ABORTED);
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.of(abortedState));

    // Act
    RecoveryExecutor.Result result =
        executor.execute(snapshotKey, selection, transactionResult, ANY_ID_3);

    // Wait for recovery to complete
    result.recoveryFuture.get();

    // Assert
    assertThat(result.recoveredResult).hasValue(prepareRolledBackResult());
    verify(recovery).recover(eq(selection), eq(transactionResult), eq(Optional.of(abortedState)));
  }

  @Test
  public void execute_CoordinatorStateIsAborted_RecordWithoutBeforeImage_ShouldRollback()
      throws Exception {
    // Arrange
    TransactionResult transactionResult =
        prepareResultWithoutBeforeImage(TransactionState.PREPARED);
    Coordinator.State abortedState = new Coordinator.State(ANY_ID_2, TransactionState.ABORTED);
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.of(abortedState));

    // Act
    RecoveryExecutor.Result result =
        executor.execute(snapshotKey, selection, transactionResult, ANY_ID_3);

    // Wait for recovery to complete
    result.recoveryFuture.get();

    // Assert
    assertThat(result.recoveredResult).isEmpty();
    verify(recovery).recover(eq(selection), eq(transactionResult), eq(Optional.of(abortedState)));
  }

  @Test
  public void execute_CoordinatorStateIsCommitted_RecordWithPreparedState_ShouldCommit()
      throws Exception {
    // Arrange
    TransactionResult transactionResult = prepareResult(TransactionState.PREPARED);
    Coordinator.State commitState = new Coordinator.State(ANY_ID_2, TransactionState.COMMITTED);
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.of(commitState));

    executor = spy(executor);
    doReturn(ANY_TIME_MILLIS_4).when(executor).getCommittedAt();

    // Act
    RecoveryExecutor.Result result =
        executor.execute(snapshotKey, selection, transactionResult, ANY_ID_3);

    // Wait for recovery to complete
    result.recoveryFuture.get();

    // Assert
    assertThat(result.recoveredResult).hasValue(prepareRolledForwardResult());
    verify(recovery).recover(eq(selection), eq(transactionResult), eq(Optional.of(commitState)));
  }

  @Test
  public void execute_CoordinatorStateIsCommitted_RecordWithDeletedState_ShouldCommit()
      throws Exception {
    // Arrange
    TransactionResult transactionResult = prepareResult(TransactionState.DELETED);
    Coordinator.State commitState = new Coordinator.State(ANY_ID_2, TransactionState.COMMITTED);
    when(coordinator.getState(ANY_ID_2)).thenReturn(Optional.of(commitState));

    executor = spy(executor);

    // Act
    RecoveryExecutor.Result result =
        executor.execute(snapshotKey, selection, transactionResult, ANY_ID_3);

    // Wait for recovery to complete
    result.recoveryFuture.get();

    // Assert
    assertThat(result.recoveredResult).isNotPresent();
    verify(recovery).recover(eq(selection), eq(transactionResult), eq(Optional.of(commitState)));
  }
}
