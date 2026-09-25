package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.StorageInfo;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.common.StorageInfoImpl;
import com.scalar.db.common.StorageInfoProvider;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Direct unit tests for {@link ParticipantCommitHandler}. */
class ParticipantCommitHandlerTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final String ANY_TEXT_4 = "text4";
  private static final String ANY_ID = "id";
  private static final String ANY_ID_2 = "id2";
  private static final int ANY_INT_1 = 100;
  private static final int ANY_INT_2 = 200;
  private static final long ANY_PREPARED_AT = 1000;
  private static final long ANY_COMMITTED_AT = 2000;

  private static final TableMetadata TABLE_METADATA =
      ConsensusCommitUtils.buildTransactionTableMetadata(
          TableMetadata.newBuilder()
              .addColumn(ANY_NAME_1, DataType.TEXT)
              .addColumn(ANY_NAME_2, DataType.TEXT)
              .addColumn(ANY_NAME_3, DataType.INT)
              .addPartitionKey(ANY_NAME_1)
              .addClusteringKey(ANY_NAME_2)
              .build());

  @Mock private DistributedStorage storage;
  @Mock private TransactionTableMetadataManager tableMetadataManager;
  @Mock private StorageInfoProvider storageInfoProvider;
  @Mock private ConsensusCommitConfig config;

  private ParallelExecutor parallelExecutor;
  private AsyncExecutor asyncExecutor;
  private MutationsGrouper mutationsGrouper;
  private ParticipantCommitHandler handler;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    parallelExecutor = new ParallelExecutor(config);
    asyncExecutor = new AsyncExecutor(config);
    mutationsGrouper = spy(new MutationsGrouper(storageInfoProvider));
    handler = newHandler(/* onePhaseCommitEnabled= */ false);

    when(storageInfoProvider.getStorageInfo(ANY_NAMESPACE_NAME))
        .thenReturn(
            new StorageInfoImpl(
                "storage1", StorageInfo.MutationAtomicityUnit.PARTITION, Integer.MAX_VALUE, false));
  }

  // Builds a ParticipantCommitHandler with the given one-phase-commit configuration. Tests that
  // exercise tryOnePhaseCommitRecords use the enabled variant; the rest use the default.
  private ParticipantCommitHandler newHandler(boolean onePhaseCommitEnabled) {
    return new ParticipantCommitHandler(
        storage,
        tableMetadataManager,
        parallelExecutor,
        asyncExecutor,
        mutationsGrouper,
        onePhaseCommitEnabled);
  }

  @AfterEach
  void tearDown() {
    asyncExecutor.close();
    parallelExecutor.close();
  }

  private Put preparePut1() {
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
        .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
        .intValue(ANY_NAME_3, ANY_INT_1)
        .build();
  }

  private Put preparePut2() {
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_3))
        .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_4))
        .intValue(ANY_NAME_3, ANY_INT_2)
        .build();
  }

  private Get prepareGet() {
    return Get.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
        .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_3))
        .build();
  }

  private Snapshot prepareSnapshot() {
    return new Snapshot(ANY_ID, tableMetadataManager, new ParallelExecutor(config));
  }

  private Snapshot prepareSnapshotWithDifferentPartitionPut() throws CrudException {
    Snapshot snapshot = prepareSnapshot();
    Put put1 = preparePut1();
    Put put2 = preparePut2();
    snapshot.putIntoWriteSet(new Snapshot.Key(put1), put1);
    snapshot.putIntoWriteSet(new Snapshot.Key(put2), put2);
    snapshot.putIntoGetSet(prepareGet(), Optional.empty());
    return snapshot;
  }

  private Delete prepareDelete2() {
    return Delete.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_3))
        .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_4))
        .build();
  }

  private Snapshot prepareSnapshotWithPutAndDelete() throws CrudException {
    Snapshot snapshot = prepareSnapshot();
    Put put = preparePut1();
    Delete delete = prepareDelete2();
    snapshot.putIntoWriteSet(new Snapshot.Key(put), put);
    snapshot.putIntoDeleteSet(new Snapshot.Key(delete), delete);
    return snapshot;
  }

  // A record this transaction has prepared, as the rollback reads it from the storage
  private Result prepareRecordPreparedByThisTransaction(
      String partitionKeyValue,
      String clusteringKeyValue,
      TransactionState state,
      boolean hasBeforeImage) {
    ImmutableMap.Builder<String, Column<?>> columns =
        ImmutableMap.<String, Column<?>>builder()
            .put(ANY_NAME_1, TextColumn.of(ANY_NAME_1, partitionKeyValue))
            .put(ANY_NAME_2, TextColumn.of(ANY_NAME_2, clusteringKeyValue))
            .put(ANY_NAME_3, IntColumn.of(ANY_NAME_3, ANY_INT_2))
            .put(Attribute.ID, TextColumn.of(Attribute.ID, ANY_ID))
            .put(Attribute.STATE, IntColumn.of(Attribute.STATE, state.get()))
            .put(Attribute.VERSION, IntColumn.of(Attribute.VERSION, hasBeforeImage ? 2 : 1));
    if (hasBeforeImage) {
      columns
          .put(
              Attribute.BEFORE_PREFIX + ANY_NAME_3,
              IntColumn.of(Attribute.BEFORE_PREFIX + ANY_NAME_3, ANY_INT_1))
          .put(Attribute.BEFORE_ID, TextColumn.of(Attribute.BEFORE_ID, ANY_ID_2))
          .put(
              Attribute.BEFORE_STATE,
              IntColumn.of(Attribute.BEFORE_STATE, TransactionState.COMMITTED.get()))
          .put(Attribute.BEFORE_VERSION, IntColumn.of(Attribute.BEFORE_VERSION, 1));
    } else {
      columns
          .put(
              Attribute.BEFORE_PREFIX + ANY_NAME_3,
              IntColumn.ofNull(Attribute.BEFORE_PREFIX + ANY_NAME_3))
          .put(Attribute.BEFORE_ID, TextColumn.ofNull(Attribute.BEFORE_ID))
          .put(Attribute.BEFORE_STATE, IntColumn.ofNull(Attribute.BEFORE_STATE))
          .put(Attribute.BEFORE_VERSION, IntColumn.ofNull(Attribute.BEFORE_VERSION));
    }
    return new ResultImpl(columns.build(), TABLE_METADATA);
  }

  // Stubs the storage with the records prepareSnapshotWithPutAndDelete() leaves prepared. The put
  // inserted a new record, which has no before image, and the delete removed an existing one,
  // which has a before image
  private void stubLatestRecordsPreparedByThisTransaction() throws ExecutionException {
    when(storage.get(ConsensusCommitUtils.createGet(new Snapshot.Key(preparePut1()))))
        .thenReturn(
            Optional.of(
                prepareRecordPreparedByThisTransaction(
                    ANY_TEXT_1, ANY_TEXT_2, TransactionState.PREPARED, false)));
    when(storage.get(ConsensusCommitUtils.createGet(new Snapshot.Key(prepareDelete2()))))
        .thenReturn(
            Optional.of(
                prepareRecordPreparedByThisTransaction(
                    ANY_TEXT_3, ANY_TEXT_4, TransactionState.DELETED, true)));
    when(tableMetadataManager.getTransactionTableMetadata(any()))
        .thenReturn(new TransactionTableMetadata(TABLE_METADATA));
  }

  // The put is rolled back by deleting the record it inserted, and the delete by restoring the
  // before image of the record it removed. Both are conditioned on this transaction and the state
  // it left the record in
  private void assertRollbackMutationsForPutAndDelete(List<Mutation> mutations) {
    assertThat(mutations).hasSize(2);
    Map<Key, Mutation> mutationsByPartitionKey =
        mutations.stream().collect(Collectors.toMap(Mutation::getPartitionKey, m -> m));

    Mutation rollbackOfPut = mutationsByPartitionKey.get(Key.ofText(ANY_NAME_1, ANY_TEXT_1));
    assertThat(rollbackOfPut).isInstanceOf(Delete.class);
    assertThat(rollbackOfPut.getClusteringKey()).hasValue(Key.ofText(ANY_NAME_2, ANY_TEXT_2));
    assertThat(rollbackOfPut.getCondition())
        .hasValue(
            ConditionBuilder.deleteIf(ConditionBuilder.column(Attribute.ID).isEqualToText(ANY_ID))
                .and(
                    ConditionBuilder.column(Attribute.STATE)
                        .isEqualToInt(TransactionState.PREPARED.get()))
                .build());

    Mutation rollbackOfDelete = mutationsByPartitionKey.get(Key.ofText(ANY_NAME_1, ANY_TEXT_3));
    assertThat(rollbackOfDelete).isInstanceOf(Put.class);
    assertThat(rollbackOfDelete.getClusteringKey()).hasValue(Key.ofText(ANY_NAME_2, ANY_TEXT_4));
    assertThat(rollbackOfDelete.getCondition())
        .hasValue(
            ConditionBuilder.putIf(ConditionBuilder.column(Attribute.ID).isEqualToText(ANY_ID))
                .and(
                    ConditionBuilder.column(Attribute.STATE)
                        .isEqualToInt(TransactionState.DELETED.get()))
                .build());
    assertThat(((Put) rollbackOfDelete).getColumns().get(ANY_NAME_3).getIntValue())
        .isEqualTo(ANY_INT_1);
  }

  private Snapshot prepareSnapshotWithoutWrites() {
    Snapshot snapshot = prepareSnapshot();
    snapshot.putIntoGetSet(prepareGet(), Optional.empty());
    return snapshot;
  }

  private TransactionContext createTransactionContext(Snapshot snapshot, Isolation isolation) {
    return new TransactionContext(ANY_ID, snapshot, isolation, false, false);
  }

  // ---------- prepareRecords ----------

  @Test
  void prepareRecords_WhenSuccessful_ShouldMutateStorage()
      throws ExecutionException, PreparationException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act
    handler.prepareRecords(context, ANY_PREPARED_AT);

    // Assert
    verify(storage, times(2)).mutate(anyList());
  }

  @SuppressWarnings("unchecked")
  @Test
  void prepareRecords_ShouldStampPassedPreparedAtOnEveryPreparedRow()
      throws ExecutionException, PreparationException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act
    handler.prepareRecords(context, ANY_PREPARED_AT);

    // Assert: every prepared row carries the single preparedAt passed in.
    ArgumentCaptor<List<Mutation>> captor = ArgumentCaptor.forClass(List.class);
    verify(storage, times(2)).mutate(captor.capture());
    List<Mutation> preparedRows =
        captor.getAllValues().stream().flatMap(List::stream).collect(Collectors.toList());
    assertThat(preparedRows).hasSize(2);
    for (Mutation mutation : preparedRows) {
      Put put = (Put) mutation;
      assertThat(put.getColumns().get(Attribute.PREPARED_AT).getBigIntValue())
          .isEqualTo(ANY_PREPARED_AT);
    }
  }

  @Test
  void prepareRecords_WhenNoMutationExceptionThrown_ShouldThrowPreparationConflictException()
      throws ExecutionException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(NoMutationException.class).when(storage).mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> handler.prepareRecords(context, ANY_PREPARED_AT))
        .isInstanceOf(PreparationConflictException.class)
        .hasCauseInstanceOf(NoMutationException.class);
  }

  @Test
  void
      prepareRecords_WhenRetriableExecutionExceptionThrown_ShouldThrowPreparationConflictException()
          throws ExecutionException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(RetriableExecutionException.class).when(storage).mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> handler.prepareRecords(context, ANY_PREPARED_AT))
        .isInstanceOf(PreparationConflictException.class)
        .hasCauseInstanceOf(RetriableExecutionException.class);
  }

  @Test
  void prepareRecords_WhenExecutionExceptionThrown_ShouldThrowPreparationException()
      throws ExecutionException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(ExecutionException.class).when(storage).mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> handler.prepareRecords(context, ANY_PREPARED_AT))
        .isInstanceOf(PreparationException.class)
        .hasCauseInstanceOf(ExecutionException.class);
  }

  // ---------- validateRecords ----------

  @Test
  void validateRecords_ValidationNotRequired_ShouldNotCallToSerializable()
      throws ValidationException, ExecutionException, CrudException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    // With SNAPSHOT isolation, validation is not required
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act
    handler.validateRecords(context);

    // Assert
    verify(snapshot, never()).toSerializable(storage);
  }

  @Test
  void validateRecords_ValidationRequired_ShouldCallToSerializable()
      throws ValidationException, ExecutionException, CrudException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(snapshot).toSerializable(storage);
    // With SERIALIZABLE isolation, validation is required when there are reads
    TransactionContext context = createTransactionContext(snapshot, Isolation.SERIALIZABLE);

    // Act
    handler.validateRecords(context);

    // Assert
    verify(snapshot).toSerializable(storage);
  }

  @Test
  void validateRecords_WhenExecutionExceptionThrown_ShouldThrowValidationException()
      throws ExecutionException, CrudException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doThrow(ExecutionException.class).when(snapshot).toSerializable(storage);
    TransactionContext context = createTransactionContext(snapshot, Isolation.SERIALIZABLE);

    // Act Assert
    assertThatThrownBy(() -> handler.validateRecords(context))
        .isInstanceOf(ValidationException.class)
        .hasCauseInstanceOf(ExecutionException.class);
  }

  // ---------- commitRecords ----------

  @Test
  void commitRecords_WhenSuccessful_ShouldMutateStorage() throws ExecutionException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act
    handler.commitRecords(context, ANY_COMMITTED_AT);

    // Assert
    verify(storage, times(2)).mutate(anyList());
  }

  @SuppressWarnings("unchecked")
  @Test
  void commitRecords_ShouldStampPassedCommittedAtOnEveryCommittedRow()
      throws ExecutionException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act
    handler.commitRecords(context, ANY_COMMITTED_AT);

    // Assert: every committed row carries the single committedAt passed in.
    ArgumentCaptor<List<Mutation>> captor = ArgumentCaptor.forClass(List.class);
    verify(storage, times(2)).mutate(captor.capture());
    List<Mutation> committedRows =
        captor.getAllValues().stream().flatMap(List::stream).collect(Collectors.toList());
    assertThat(committedRows).hasSize(2);
    for (Mutation mutation : committedRows) {
      Put put = (Put) mutation;
      assertThat(put.getColumns().get(Attribute.COMMITTED_AT).getBigIntValue())
          .isEqualTo(ANY_COMMITTED_AT);
    }
  }

  @Test
  void commitRecords_WhenAsyncCommitEnabled_ShouldCommitRecordsInBackground() throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    AtomicReference<Thread> mutateThread = new AtomicReference<>();
    doAnswer(
            invocation -> {
              mutateThread.set(Thread.currentThread());
              return null;
            })
        .when(storage)
        .mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    when(config.isAsyncCommitEnabled()).thenReturn(true);
    when(config.getParallelExecutorCount()).thenReturn(4);
    AsyncExecutor enabledAsyncExecutor = new AsyncExecutor(config);
    ParticipantCommitHandler asyncHandler =
        new ParticipantCommitHandler(
            storage,
            tableMetadataManager,
            parallelExecutor,
            enabledAsyncExecutor,
            mutationsGrouper,
            false);

    try {
      // Act
      asyncHandler.commitRecords(context, ANY_COMMITTED_AT);

      // Assert
      verify(storage, timeout(10000).times(2)).mutate(anyList());
      assertThat(mutateThread.get()).isNotEqualTo(Thread.currentThread());
    } finally {
      enabledAsyncExecutor.close();
    }
  }

  @Test
  void commitRecords_WhenStorageThrows_ShouldNotPropagateException()
      throws ExecutionException, CrudException {
    // Lazy recovery picks up failed commits, so commitRecords ignores storage failures.
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(ExecutionException.class).when(storage).mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act (must not throw)
    handler.commitRecords(context, ANY_COMMITTED_AT);
  }

  // ---------- rollbackRecords ----------

  @Test
  void rollbackRecords_WhenSuccessful_ShouldDriveSnapshotThroughComposer()
      throws ExecutionException, CrudException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(storage).mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act
    handler.rollbackRecords(context);

    // Assert
    verify(snapshot).to(any(RollbackMutationComposer.class));
  }

  @Test
  void rollbackRecords_WhenSuccessful_ShouldReadLatestRecordOfEveryWriteThroughParallelExecutor()
      throws ExecutionException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    when(storage.get(any(Get.class))).thenReturn(Optional.empty());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);
    ParallelExecutor spiedParallelExecutor = spy(parallelExecutor);
    ParticipantCommitHandler handlerWithSpiedParallelExecutor =
        new ParticipantCommitHandler(
            storage,
            tableMetadataManager,
            spiedParallelExecutor,
            asyncExecutor,
            mutationsGrouper,
            false);

    // Act
    handlerWithSpiedParallelExecutor.rollbackRecords(context);

    // Assert

    // The snapshot has two writes, and the latest record of each is read once through the parallel
    // executor. The composer does not read them again
    verify(spiedParallelExecutor)
        .readRecordsForRollback(argThat(tasks -> tasks.size() == 2), eq(ANY_ID));
    verify(storage, times(2)).get(any(Get.class));
  }

  @SuppressWarnings("unchecked")
  @Test
  void rollbackRecords_WhenLatestRecordsArePreparedByThisTransaction_ShouldRollBackWriteAndDelete()
      throws ExecutionException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithPutAndDelete();
    stubLatestRecordsPreparedByThisTransaction();
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act
    handler.rollbackRecords(context);

    // Assert
    ArgumentCaptor<List<Mutation>> captor = ArgumentCaptor.forClass(List.class);
    verify(storage, times(2)).mutate(captor.capture());
    assertRollbackMutationsForPutAndDelete(
        captor.getAllValues().stream().flatMap(List::stream).collect(Collectors.toList()));
  }

  @SuppressWarnings("unchecked")
  @Test
  void rollbackRecords_WhenAsyncRollbackEnabled_ShouldRollBackWriteAndDeleteInBackground()
      throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithPutAndDelete();
    stubLatestRecordsPreparedByThisTransaction();
    AtomicReference<Thread> mutateThread = new AtomicReference<>();
    doAnswer(
            invocation -> {
              mutateThread.set(Thread.currentThread());
              return null;
            })
        .when(storage)
        .mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    when(config.isAsyncRollbackEnabled()).thenReturn(true);
    when(config.getParallelExecutorCount()).thenReturn(4);
    AsyncExecutor enabledAsyncExecutor = new AsyncExecutor(config);
    ParticipantCommitHandler asyncHandler =
        new ParticipantCommitHandler(
            storage,
            tableMetadataManager,
            parallelExecutor,
            enabledAsyncExecutor,
            mutationsGrouper,
            false);

    try {
      // Act
      asyncHandler.rollbackRecords(context);

      // Assert
      verify(storage, timeout(10000).times(2)).mutate(anyList());
      assertThat(mutateThread.get()).isNotEqualTo(Thread.currentThread());
      ArgumentCaptor<List<Mutation>> captor = ArgumentCaptor.forClass(List.class);
      verify(storage, times(2)).mutate(captor.capture());
      assertRollbackMutationsForPutAndDelete(
          captor.getAllValues().stream().flatMap(List::stream).collect(Collectors.toList()));
    } finally {
      enabledAsyncExecutor.close();
    }
  }

  @Test
  void rollbackRecords_WhenReadingLatestRecordThrows_ShouldNotMutateAndNotPropagateException()
      throws ExecutionException, CrudException {
    // A failed read leaves the rollback to the lazy recovery, as a failed rollback mutation does

    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    when(storage.get(any(Get.class))).thenThrow(ExecutionException.class);
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act (must not throw)
    handler.rollbackRecords(context);

    // Assert
    verify(storage, never()).mutate(anyList());
  }

  @Test
  void rollbackRecords_WhenStorageThrows_ShouldNotPropagateException()
      throws ExecutionException, CrudException {
    // Lazy recovery picks up failed rollbacks, so rollbackRecords ignores storage failures.
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(ExecutionException.class).when(storage).mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act (must not throw)
    handler.rollbackRecords(context);
  }

  // ---------- canOnePhaseCommit ----------

  @Test
  void canOnePhaseCommit_WhenOnePhaseCommitDisabled_ShouldReturnFalse() throws Exception {
    // Arrange — default handler has onePhaseCommitEnabled = false.
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThat(handler.canOnePhaseCommit(context)).isFalse();
    verify(mutationsGrouper, never()).canBeGroupedAltogether(anyList());
  }

  @Test
  void canOnePhaseCommit_WhenSerializableIsolationWithReads_ShouldReturnFalse() throws Exception {
    // SERIALIZABLE + reads requires validation, which one-phase commit cannot satisfy.
    // Arrange
    ParticipantCommitHandler enabled = newHandler(/* onePhaseCommitEnabled= */ true);
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    TransactionContext context = createTransactionContext(snapshot, Isolation.SERIALIZABLE);

    // Act Assert
    assertThat(enabled.canOnePhaseCommit(context)).isFalse();
  }

  @Test
  void canOnePhaseCommit_WhenNoWritesAndDeletes_ShouldReturnFalse() throws Exception {
    // Arrange
    ParticipantCommitHandler enabled = newHandler(/* onePhaseCommitEnabled= */ true);
    Snapshot snapshot = prepareSnapshotWithoutWrites();
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThat(enabled.canOnePhaseCommit(context)).isFalse();
  }

  @Test
  void canOnePhaseCommit_WhenDeleteWithoutExistingRecord_ShouldReturnFalse() throws Exception {
    // A delete with no corresponding record in the read set means we cannot detect conflicts
    // through delete-if-exists semantics.
    // Arrange
    ParticipantCommitHandler enabled = newHandler(/* onePhaseCommitEnabled= */ true);
    Snapshot snapshot = prepareSnapshot();
    Delete delete =
        Delete.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(Key.ofText(ANY_NAME_1, ANY_TEXT_1))
            .clusteringKey(Key.ofText(ANY_NAME_2, ANY_TEXT_2))
            .build();
    snapshot.putIntoDeleteSet(new Snapshot.Key(delete), delete);
    snapshot.putIntoReadSet(new Snapshot.Key(delete), Optional.empty());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThat(enabled.canOnePhaseCommit(context)).isFalse();
  }

  @Test
  void canOnePhaseCommit_WhenMutationsCannotBeGrouped_ShouldReturnFalse() throws Exception {
    // Arrange
    ParticipantCommitHandler enabled = newHandler(/* onePhaseCommitEnabled= */ true);
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doReturn(false).when(mutationsGrouper).canBeGroupedAltogether(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThat(enabled.canOnePhaseCommit(context)).isFalse();
  }

  @Test
  void canOnePhaseCommit_WhenEligible_ShouldReturnTrue() throws Exception {
    // Arrange
    ParticipantCommitHandler enabled = newHandler(/* onePhaseCommitEnabled= */ true);
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doReturn(true).when(mutationsGrouper).canBeGroupedAltogether(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThat(enabled.canOnePhaseCommit(context)).isTrue();
  }

  @Test
  void canOnePhaseCommit_WhenMutationsGrouperThrowsExecutionException_ShouldThrowCommitException()
      throws Exception {
    // Arrange
    ParticipantCommitHandler enabled = newHandler(/* onePhaseCommitEnabled= */ true);
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(ExecutionException.class).when(mutationsGrouper).canBeGroupedAltogether(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> enabled.canOnePhaseCommit(context))
        .isInstanceOf(CommitException.class)
        .hasCauseInstanceOf(ExecutionException.class);
  }

  // ---------- onePhaseCommitRecords ----------

  @Test
  void onePhaseCommitRecords_WhenSuccessful_ShouldMutateStorage() throws Exception {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(storage).mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act
    handler.onePhaseCommitRecords(context);

    // Assert
    verify(storage).mutate(anyList());
    verify(snapshot).to(any(OnePhaseCommitMutationComposer.class));
  }

  @Test
  void onePhaseCommitRecords_WhenNoMutationException_ShouldThrowCommitConflictException()
      throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(NoMutationException.class).when(storage).mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> handler.onePhaseCommitRecords(context))
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(NoMutationException.class);
  }

  @Test
  void onePhaseCommitRecords_WhenRetriableExecutionException_ShouldThrowCommitConflictException()
      throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(RetriableExecutionException.class).when(storage).mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> handler.onePhaseCommitRecords(context))
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(RetriableExecutionException.class);
  }

  @Test
  void onePhaseCommitRecords_WhenExecutionException_ShouldThrowUnknownTransactionStatusException()
      throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(ExecutionException.class).when(storage).mutate(anyList());
    TransactionContext context = createTransactionContext(snapshot, Isolation.SNAPSHOT);

    // Act Assert
    assertThatThrownBy(() -> handler.onePhaseCommitRecords(context))
        .isInstanceOf(UnknownTransactionStatusException.class)
        .hasCauseInstanceOf(ExecutionException.class);
  }
}
