package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.StorageInfo;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.StorageInfoImpl;
import com.scalar.db.common.StorageInfoProvider;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CommitHandlerTest {
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
  private static final int ANY_INT_1 = 100;
  private static final int ANY_INT_2 = 200;

  @Mock protected DistributedStorage storage;
  @Mock protected Coordinator coordinator;
  @Mock protected TransactionTableMetadataManager tableMetadataManager;
  @Mock protected StorageInfoProvider storageInfoProvider;
  @Mock protected ConsensusCommitConfig config;
  @Mock protected BeforePreparationHook beforePreparationHook;
  @Mock protected Future<Void> beforePreparationHookFuture;

  private CommitHandler handler;
  protected ParallelExecutor parallelExecutor;
  protected MutationsGrouper mutationsGrouper;

  protected String anyId() {
    return ANY_ID;
  }

  // Creates a TransactionContext for tests. Overridden by the group commit test to mark the context
  // as holding a reserved group commit slot, mirroring what ConsensusCommitManager.begin() does.
  protected TransactionContext createTransactionContext(
      String id, Snapshot snapshot, Isolation isolation, boolean readOnly, boolean oneOperation) {
    return new TransactionContext(id, snapshot, isolation, readOnly, oneOperation);
  }

  protected void extraInitialize() {}

  protected void extraCleanup() {}

  protected CommitHandler createCommitHandler(boolean coordinatorWriteOmissionOnReadOnlyEnabled) {
    return new CommitHandler(
        storage,
        coordinator,
        tableMetadataManager,
        parallelExecutor,
        new MutationsGrouper(storageInfoProvider),
        coordinatorWriteOmissionOnReadOnlyEnabled,
        false);
  }

  protected CommitHandler createCommitHandlerWithOnePhaseCommit() {
    return new CommitHandler(
        storage, coordinator, tableMetadataManager, parallelExecutor, mutationsGrouper, true, true);
  }

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    parallelExecutor = new ParallelExecutor(config);
    handler = spy(createCommitHandler(true));
    mutationsGrouper = spy(new MutationsGrouper(storageInfoProvider));

    extraInitialize();

    // Arrange
    when(storageInfoProvider.getStorageInfo(ANY_NAMESPACE_NAME))
        .thenReturn(
            new StorageInfoImpl(
                "storage1", StorageInfo.MutationAtomicityUnit.PARTITION, Integer.MAX_VALUE, false));
  }

  @AfterEach
  void tearDown() {
    extraCleanup();

    parallelExecutor.close();
  }

  private Put preparePut1() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_2);
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .intValue(ANY_NAME_3, ANY_INT_1)
        .build();
  }

  private Put preparePut2() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_3);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_4);
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .intValue(ANY_NAME_3, ANY_INT_2)
        .build();
  }

  private Put preparePut3() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_3);
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .intValue(ANY_NAME_3, ANY_INT_2)
        .build();
  }

  private Get prepareGet() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_3);
    return Get.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
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

  protected Snapshot prepareSnapshotWithDifferentPartitionPut() throws CrudException {
    Snapshot snapshot = prepareSnapshot();

    // different partition
    Put put1 = preparePut1();
    Put put2 = preparePut2();
    snapshot.putIntoWriteSet(new Snapshot.Key(put1), put1);
    snapshot.putIntoWriteSet(new Snapshot.Key(put2), put2);

    Get get = prepareGet();
    snapshot.putIntoGetSet(get, Optional.empty());

    return snapshot;
  }

  protected Snapshot prepareSnapshotWithSamePartitionPut() throws CrudException {
    Snapshot snapshot = prepareSnapshot();

    // same partition
    Put put1 = preparePut1();
    Put put3 = preparePut3();
    snapshot.putIntoWriteSet(new Snapshot.Key(put1), put1);
    snapshot.putIntoWriteSet(new Snapshot.Key(put3), put3);

    Get get = prepareGet();
    snapshot.putIntoGetSet(get, Optional.empty());

    return snapshot;
  }

  protected Snapshot prepareSnapshotWithoutWrites() {
    Snapshot snapshot = prepareSnapshot();

    Get get = prepareGet();
    snapshot.putIntoGetSet(get, Optional.empty());

    return snapshot;
  }

  private Snapshot prepareSnapshot() {
    return new Snapshot(anyId(), tableMetadataManager, new ParallelExecutor(config));
  }

  private void setBeforePreparationHookIfNeeded(boolean withBeforePreparationHook) {
    if (withBeforePreparationHook) {
      doReturn(beforePreparationHookFuture).when(beforePreparationHook).handle(any(), any());
      handler.setBeforePreparationHook(beforePreparationHook);
    }
  }

  // Test helper: encode the WriteSet that production would persist for a single-group transaction
  // commit/abort whose snapshot is `snapshot`. The WriteSet output depends only on the snapshot
  // content (not on the transaction id), so wrapping in a fresh TransactionContext is sufficient.
  private com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet expectedSingleGroupWriteSet(
      Snapshot snapshot) {
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);
    return handler.writeSetEncoder.encodeSingleGroupWriteSet(context, false);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void commit_SnapshotWithDifferentPartitionPutsGiven_ShouldCommitRespectively(
      boolean withBeforePreparationHook)
      throws CommitException, UnknownTransactionStatusException, ExecutionException,
          CoordinatorException, ValidationConflictException, CrudException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(storage).mutate(anyList());
    doNothingWhenCoordinatorPutState();
    setBeforePreparationHookIfNeeded(withBeforePreparationHook);
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.commit(context);

    // Assert
    verify(storage, times(4)).mutate(anyList());
    verify(snapshot, never()).toSerializable(storage);
    verifyCoordinatorPutState(TransactionState.COMMITTED, snapshot);
    verifyBeforePreparationHook(withBeforePreparationHook, context);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void commit_SnapshotWithSamePartitionPutsGiven_ShouldCommitAtOnce(
      boolean withBeforePreparationHook)
      throws CommitException, UnknownTransactionStatusException, ExecutionException,
          CoordinatorException, ValidationConflictException, CrudException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithSamePartitionPut());
    doNothing().when(storage).mutate(anyList());
    doNothingWhenCoordinatorPutState();
    setBeforePreparationHookIfNeeded(withBeforePreparationHook);
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.commit(context);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verify(snapshot, never()).toSerializable(storage);
    verifyCoordinatorPutState(TransactionState.COMMITTED, snapshot);
    verifyBeforePreparationHook(withBeforePreparationHook, context);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void commit_InReadOnlyMode_ShouldNotPrepareRecordsAndCommitStateAndCommitRecords(
      boolean withBeforePreparationHook)
      throws CommitException, UnknownTransactionStatusException, ExecutionException,
          CoordinatorException, ValidationConflictException, CrudException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithoutWrites());
    setBeforePreparationHookIfNeeded(withBeforePreparationHook);
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, true, false);

    // Act
    handler.commit(context);

    // Assert
    verify(storage, never()).mutate(anyList());
    verify(snapshot, never()).toSerializable(storage);
    verify(coordinator, never()).putState(any());
    verifyBeforePreparationHook(withBeforePreparationHook, context);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void
      commit_NoWritesAndDeletesInSnapshot_ShouldNotPrepareRecordsAndCommitStateAndCommitRecords(
          boolean withBeforePreparationHook)
          throws CommitException, UnknownTransactionStatusException, ExecutionException,
              CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithoutWrites());
    setBeforePreparationHookIfNeeded(withBeforePreparationHook);
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.commit(context);

    // Assert
    verify(storage, never()).mutate(anyList());
    verify(snapshot, never()).toSerializable(storage);
    verify(coordinator, never()).putState(any());
    verifyBeforePreparationHook(withBeforePreparationHook, context);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void
      commit_NoWritesAndDeletesInSnapshot_CoordinatorWriteOmissionOnReadOnlyDisabled_ShouldNotPrepareRecordsAndCommitRecordsButShouldCommitState(
          boolean withBeforePreparationHook)
          throws CommitException, UnknownTransactionStatusException, ExecutionException,
              CoordinatorException, ValidationConflictException {
    // Arrange
    handler = spy(createCommitHandler(false));
    Snapshot snapshot = spy(prepareSnapshotWithoutWrites());
    setBeforePreparationHookIfNeeded(withBeforePreparationHook);
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.commit(context);

    // Assert
    verify(storage, never()).mutate(anyList());
    verify(snapshot, never()).toSerializable(storage);
    verifyCoordinatorPutState(TransactionState.COMMITTED, snapshot);
    verifyBeforePreparationHook(withBeforePreparationHook, context);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void
      commit_NoWritesAndDeletesInSnapshot_ValidationFailed_ShouldNotPrepareRecordsAndAbortStateAndRollbackRecords(
          boolean withBeforePreparationHook)
          throws ExecutionException, CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithoutWrites());
    setBeforePreparationHookIfNeeded(withBeforePreparationHook);
    doThrow(ValidationConflictException.class).when(snapshot).toSerializable(storage);
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitConflictException.class);

    // Assert
    verify(storage, never()).mutate(anyList());
    verify(snapshot).toSerializable(storage);
    verify(coordinator, never()).putState(any());
    verifyBeforePreparationHook(withBeforePreparationHook, context);
    verify(handler).onFailureBeforeCommit(any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void
      commit_NoWritesAndDeletesInSnapshot_ValidationFailed_CoordinatorWriteOmissionOnReadOnlyDisabled_ShouldNotPrepareRecordsAndRollbackRecordsButShouldAbortState(
          boolean withBeforePreparationHook)
          throws ExecutionException, CoordinatorException, ValidationConflictException {
    // Arrange
    handler = spy(createCommitHandler(false));
    Snapshot snapshot = spy(prepareSnapshotWithoutWrites());
    setBeforePreparationHookIfNeeded(withBeforePreparationHook);
    doThrow(ValidationConflictException.class).when(snapshot).toSerializable(storage);
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitConflictException.class);

    // Assert
    verify(storage, never()).mutate(anyList());
    verify(snapshot).toSerializable(storage);
    verify(coordinator).putState(any());
    verifyBeforePreparationHook(withBeforePreparationHook, context);
    verify(handler).onFailureBeforeCommit(any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void commit_SnapshotIsolationWithReads_ShouldNotValidateRecords(
      boolean withBeforePreparationHook)
      throws CommitException, UnknownTransactionStatusException, ExecutionException,
          CoordinatorException, ValidationConflictException, CrudException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(storage).mutate(anyList());
    doNothingWhenCoordinatorPutState();
    setBeforePreparationHookIfNeeded(withBeforePreparationHook);
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.commit(context);

    // Assert
    verify(storage, times(4)).mutate(anyList());
    // With SNAPSHOT isolation, validation should not be performed even if there are reads
    verify(snapshot, never()).toSerializable(storage);
    verifyCoordinatorPutState(TransactionState.COMMITTED, snapshot);
    verifyBeforePreparationHook(withBeforePreparationHook, context);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void commit_SerializableIsolationWithReads_ShouldValidateRecords(
      boolean withBeforePreparationHook)
      throws CommitException, UnknownTransactionStatusException, ExecutionException,
          CoordinatorException, ValidationConflictException, CrudException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(storage).mutate(anyList());
    doNothing().when(snapshot).toSerializable(storage);
    doNothingWhenCoordinatorPutState();
    setBeforePreparationHookIfNeeded(withBeforePreparationHook);
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act
    handler.commit(context);

    // Assert
    verify(storage, times(4)).mutate(anyList());
    // With SERIALIZABLE isolation, validation should be performed when there are reads
    verify(snapshot).toSerializable(storage);
    verifyCoordinatorPutState(TransactionState.COMMITTED, snapshot);
    verifyBeforePreparationHook(withBeforePreparationHook, context);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @Test
  public void validateRecords_ValidationNotRequired_ShouldNotCallToSerializable()
      throws ValidationException, ExecutionException, CrudException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    // With SNAPSHOT isolation, validation is not required
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.validateRecords(context);

    // Assert
    verify(snapshot, never()).toSerializable(storage);
  }

  @Test
  public void validateRecords_ValidationRequired_ShouldCallToSerializable()
      throws ValidationException, ExecutionException, CrudException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(snapshot).toSerializable(storage);
    // With SERIALIZABLE isolation, validation is required when there are reads
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act
    handler.validateRecords(context);

    // Assert
    verify(snapshot).toSerializable(storage);
  }

  @Test
  public void commit_NoMutationExceptionThrownInPrepareRecords_ShouldThrowCCException()
      throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(NoMutationException.class).when(storage).mutate(anyList());
    doNothing().when(coordinator).putState(any(Coordinator.State.class));
    doNothing().when(handler).rollbackRecords(any(TransactionContext.class));
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitConflictException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator)
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.COMMITTED));
    verify(handler).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void commit_RetriableExecutionExceptionThrownInPrepareRecords_ShouldThrowCCException()
      throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(RetriableExecutionException.class).when(storage).mutate(anyList());
    doNothing().when(coordinator).putState(any(Coordinator.State.class));
    doNothing().when(handler).rollbackRecords(any(TransactionContext.class));
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitConflictException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator)
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.COMMITTED));
    verify(handler).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void commit_ExceptionThrownInPrepareRecords_ShouldAbortAndRollbackRecords()
      throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(ExecutionException.class).when(storage).mutate(anyList());
    doNothing().when(coordinator).putState(any(Coordinator.State.class));
    doNothing().when(handler).rollbackRecords(any(TransactionContext.class));
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator)
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.COMMITTED));
    verify(handler).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void
      commit_ExceptionThrownInPrepareRecordsAndCoordinatorConflictExceptionThrownInCoordinatorAbortThenAbortedReturnedInGetState_ShouldRollbackRecords()
          throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(ExecutionException.class).when(storage).mutate(anyList());
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.ABORTED));
    doReturn(
            Optional.of(
                new Coordinator.State(
                    anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.ABORTED)))
        .when(coordinator)
        .getState(anyId());
    doNothing().when(handler).rollbackRecords(any(TransactionContext.class));
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator)
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.COMMITTED));
    verify(coordinator).getState(anyId());
    verify(handler).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void abortStateForRollback_ShouldPutStateForLazyRecoveryRollbackAndReturnAborted()
      throws CoordinatorException, UnknownTransactionStatusException {
    // Arrange
    doNothing().when(coordinator).putStateForLazyRecoveryRollback(anyId());

    // Act
    TransactionState result = handler.abortStateForRollback(anyId());

    // Assert
    // The abort delegates to Coordinator#putStateForLazyRecoveryRollback rather than a plain
    // putState. That method branches on the ID: it writes the parent-ID conflict marker before the
    // full-ID record for a group commit full key (so the abort wins against an in-flight group
    // commit), and falls back to a single putState for a non-group-commit ID. Which branch runs
    // here depends on anyId(): a non-group-commit ID in this base class, a group commit full key in
    // the group commit subclass that overrides anyId().
    assertThat(result).isEqualTo(TransactionState.ABORTED);
    verify(coordinator).putStateForLazyRecoveryRollback(anyId());
    verify(coordinator, never()).putState(any());
  }

  @Test
  public void abortStateForRollback_WhenConflictAndCommittedStatePersisted_ShouldReturnCommitted()
      throws CoordinatorException, UnknownTransactionStatusException {
    // Arrange
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putStateForLazyRecoveryRollback(anyId());
    doReturn(Optional.of(new Coordinator.State(anyId(), TransactionState.COMMITTED)))
        .when(coordinator)
        .getState(anyId());

    // Act
    TransactionState result = handler.abortStateForRollback(anyId());

    // Assert
    // A concurrent writer committed first, so the persisted COMMITTED state is followed.
    assertThat(result).isEqualTo(TransactionState.COMMITTED);
    verify(coordinator).getState(anyId());
  }

  @Test
  public void abortStateForRollback_WhenConflictAndNoStatePersisted_ShouldThrowUnknown()
      throws CoordinatorException {
    // The conflicting state row is absent on re-read, which is reachable once the Coordinator state
    // cleanup process removes a finished row. The outcome cannot be recovered, so an honest
    // UnknownTransactionStatusException is thrown, preserving the original conflict as the cause.

    // Arrange
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putStateForLazyRecoveryRollback(anyId());
    doReturn(Optional.empty()).when(coordinator).getState(anyId());

    // Act Assert
    assertThatThrownBy(() -> handler.abortStateForRollback(anyId()))
        .isInstanceOf(UnknownTransactionStatusException.class)
        .hasCauseInstanceOf(CoordinatorConflictException.class);
    verify(coordinator).getState(anyId());
  }

  @Test
  public void abortStateForRollback_WhenCoordinatorExceptionThrown_ShouldThrowUnknown()
      throws CoordinatorException {
    // Arrange
    doThrow(CoordinatorException.class).when(coordinator).putStateForLazyRecoveryRollback(anyId());

    // Act Assert
    assertThatThrownBy(() -> handler.abortStateForRollback(anyId()))
        .isInstanceOf(UnknownTransactionStatusException.class);
  }

  @Test
  public void
      abortStateWithoutWriteSet_WhenConflictAndCommittedStatePersisted_ShouldReturnCommitted()
          throws CoordinatorException, UnknownTransactionStatusException {
    // Arrange
    doThrow(CoordinatorConflictException.class).when(coordinator).putState(any());
    doReturn(Optional.of(new Coordinator.State(anyId(), TransactionState.COMMITTED)))
        .when(coordinator)
        .getState(anyId());

    // Act
    TransactionState result = handler.abortStateWithoutWriteSet(anyId());

    // Assert
    // A concurrent writer committed first, so the persisted COMMITTED state is followed.
    assertThat(result).isEqualTo(TransactionState.COMMITTED);
    verify(coordinator).getState(anyId());
  }

  @Test
  public void abortStateWithoutWriteSet_WhenConflictAndNoStatePersisted_ShouldReturnAborted()
      throws CoordinatorException, UnknownTransactionStatusException {
    // Unlike abortStateForRollback (the One-phase Commit I/F abort-by-id path), this method is used
    // by the self-abort path and all Two-phase Commit I/F aborts, which cannot be racing a real,
    // deletable COMMITTED row. A conflicting-then-absent coordinator state is therefore
    // determinable as ABORTED (the conflict was a lazy-recovery ABORTED later removed by the
    // Coordinator state cleanup process), not an honest UNKNOWN.

    // Arrange
    doThrow(CoordinatorConflictException.class).when(coordinator).putState(any());
    doReturn(Optional.empty()).when(coordinator).getState(anyId());

    // Act
    TransactionState result = handler.abortStateWithoutWriteSet(anyId());

    // Assert
    assertThat(result).isEqualTo(TransactionState.ABORTED);
    verify(coordinator).getState(anyId());
  }

  @Test
  public void
      commit_ExceptionThrownInPrepareRecordsAndCoordinatorConflictExceptionThrownInCoordinatorAbortThenNothingReturnedInGetState_ShouldRollbackRecordsAndThrowCommitException()
          throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(ExecutionException.class).when(storage).mutate(anyList());
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.ABORTED));
    doReturn(Optional.empty()).when(coordinator).getState(anyId());
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act

    // The self-abort path provably never committed, so a conflicting-then-absent coordinator state
    // resolves to ABORTED rather than UNKNOWN. The commit then surfaces the original preparation
    // failure (a CommitException) instead of masking it with an UnknownTransactionStatusException.
    assertThatThrownBy(() -> handler.commit(context))
        .isInstanceOf(CommitException.class)
        .isNotInstanceOf(UnknownTransactionStatusException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator)
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.COMMITTED));
    verify(coordinator).getState(anyId());
    verify(handler).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void
      commit_ExceptionThrownInPrepareRecordsAndCoordinatorConflictExceptionThrownInCoordinatorAbortThenExceptionThrownInGetState_ShouldThrowUnknown()
          throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(ExecutionException.class).when(storage).mutate(anyList());
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.ABORTED));
    doThrow(CoordinatorException.class).when(coordinator).getState(anyId());
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator)
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.COMMITTED));
    verify(coordinator).getState(anyId());
    verify(handler, never()).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void
      commit_ExceptionThrownInPrepareRecordsAndCoordinatorExceptionThrownInCoordinatorAbort_ShouldThrowUnknown()
          throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(ExecutionException.class).when(storage).mutate(anyList());
    doThrow(CoordinatorException.class)
        .when(coordinator)
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.ABORTED));
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator)
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.COMMITTED));
    verify(handler, never()).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void commit_ValidationConflictExceptionThrownInValidation_ShouldAbortAndRollbackRecords()
      throws ExecutionException, CoordinatorException, ValidationConflictException, CrudException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(storage).mutate(anyList());
    doThrow(ValidationConflictException.class).when(snapshot).toSerializable(storage);
    doNothing().when(coordinator).putState(any(Coordinator.State.class));
    doNothing().when(handler).rollbackRecords(any(TransactionContext.class));
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator)
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.COMMITTED));
    verify(handler).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void commit_ExceptionThrownInValidation_ShouldAbortAndRollbackRecords()
      throws ExecutionException, CoordinatorException, ValidationConflictException, CrudException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(storage).mutate(anyList());
    doThrow(ExecutionException.class).when(snapshot).toSerializable(storage);
    doNothing().when(coordinator).putState(any(Coordinator.State.class));
    doNothing().when(handler).rollbackRecords(any(TransactionContext.class));
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator)
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.COMMITTED));
    verify(handler).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void
      commit_ExceptionThrownInValidationAndCoordinatorConflictExceptionThrownInCoordinatorAbortThenAbortedReturnedInGetState_ShouldRollbackRecords()
          throws ExecutionException, CoordinatorException, ValidationConflictException,
              CrudException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    WriteSet writeSet = expectedSingleGroupWriteSet(snapshot);
    doNothing().when(storage).mutate(anyList());
    doThrow(ExecutionException.class).when(snapshot).toSerializable(storage);
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(new Coordinator.State(anyId(), writeSet, TransactionState.ABORTED));
    doReturn(Optional.of(new Coordinator.State(anyId(), writeSet, TransactionState.ABORTED)))
        .when(coordinator)
        .getState(anyId());
    doNothing().when(handler).rollbackRecords(any(TransactionContext.class));
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator)
        .putState(new Coordinator.State(anyId(), writeSet, TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), writeSet, TransactionState.COMMITTED));
    verify(coordinator).getState(anyId());
    verify(handler).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void
      commit_ExceptionThrownInValidationAndCoordinatorConflictExceptionThrownInCoordinatorAbortThenNothingReturnedInGetState_ShouldRollbackRecordsAndThrowCommitException()
          throws ExecutionException, CoordinatorException, ValidationConflictException,
              CrudException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    WriteSet writeSet = expectedSingleGroupWriteSet(snapshot);
    doNothing().when(storage).mutate(anyList());
    doThrow(ExecutionException.class).when(snapshot).toSerializable(storage);
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(new Coordinator.State(anyId(), writeSet, TransactionState.ABORTED));
    doReturn(Optional.empty()).when(coordinator).getState(anyId());
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act

    // The self-abort path provably never committed, so a conflicting-then-absent coordinator state
    // resolves to ABORTED rather than UNKNOWN. The commit then surfaces the original validation
    // failure (a CommitException) instead of masking it with an UnknownTransactionStatusException.
    assertThatThrownBy(() -> handler.commit(context))
        .isInstanceOf(CommitException.class)
        .isNotInstanceOf(UnknownTransactionStatusException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator)
        .putState(new Coordinator.State(anyId(), writeSet, TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), writeSet, TransactionState.COMMITTED));
    verify(coordinator).getState(anyId());
    verify(handler).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void
      commit_ExceptionThrownInValidationAndCoordinatorConflictExceptionThrownInCoordinatorAbortThenExceptionThrownInGetState_ShouldThrowUnknown()
          throws ExecutionException, CoordinatorException, ValidationConflictException,
              CrudException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    WriteSet writeSet = expectedSingleGroupWriteSet(snapshot);
    doNothing().when(storage).mutate(anyList());
    doThrow(ExecutionException.class).when(snapshot).toSerializable(storage);
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(new Coordinator.State(anyId(), writeSet, TransactionState.ABORTED));
    doThrow(CoordinatorException.class).when(coordinator).getState(anyId());
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator)
        .putState(new Coordinator.State(anyId(), writeSet, TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), writeSet, TransactionState.COMMITTED));
    verify(coordinator).getState(anyId());
    verify(handler, never()).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void
      commit_ExceptionThrownInValidationAndCoordinatorExceptionThrownInCoordinatorAbort_ShouldThrowUnknown()
          throws ExecutionException, CoordinatorException, ValidationConflictException,
              CrudException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    WriteSet writeSet = expectedSingleGroupWriteSet(snapshot);
    doNothing().when(storage).mutate(anyList());
    doThrow(ExecutionException.class).when(snapshot).toSerializable(storage);
    doThrow(CoordinatorException.class)
        .when(coordinator)
        .putState(new Coordinator.State(anyId(), writeSet, TransactionState.ABORTED));
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator)
        .putState(new Coordinator.State(anyId(), writeSet, TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), writeSet, TransactionState.COMMITTED));
    verify(handler, never()).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void
      commit_CoordinatorConflictExceptionThrownInCoordinatorCommitAndThenCommittedReturnedInGetState_ShouldBeIgnored()
          throws ExecutionException, CoordinatorException, CommitException,
              UnknownTransactionStatusException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    doThrowExceptionWhenCoordinatorPutState(
        TransactionState.COMMITTED, CoordinatorConflictException.class, snapshot);
    doReturn(
            Optional.of(
                new Coordinator.State(
                    anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.COMMITTED)))
        .when(coordinator)
        .getState(anyId());
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.commit(context);

    // Assert
    verify(storage, times(4)).mutate(anyList());
    verifyCoordinatorPutState(TransactionState.COMMITTED, snapshot);
    verify(coordinator).getState(anyId());
    verify(handler, never()).rollbackRecords(context);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @Test
  public void
      commit_CoordinatorConflictExceptionThrownInCoordinatorCommitAndThenAbortedReturnedInGetState_ShouldRollbackRecords()
          throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    doThrowExceptionWhenCoordinatorPutState(
        TransactionState.COMMITTED, CoordinatorConflictException.class, snapshot);
    doReturn(
            Optional.of(
                new Coordinator.State(
                    anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.ABORTED)))
        .when(coordinator)
        .getState(anyId());
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verifyCoordinatorPutState(TransactionState.COMMITTED, snapshot);
    verify(coordinator).getState(anyId());
    verify(handler).rollbackRecords(context);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @Test
  public void
      commit_CoordinatorConflictExceptionThrownInCoordinatorCommitAndThenNothingReturnedInGetState_ShouldRollbackRecordsAndThrowCommitConflict()
          throws ExecutionException, CoordinatorException, CrudException {
    // Writing the COMMITTED state conflicts, and the conflicting row is absent on re-read. The only
    // concurrent writer of this transaction's state is a lazy recovery, which only ever writes
    // ABORTED, so the conflicting row was an ABORTED state that the Coordinator state cleanup
    // process then removed. The transaction is therefore aborted, so the records are rolled back
    // and a conflict is reported -- the same outcome as when the ABORTED state is still present.

    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    doThrowExceptionWhenCoordinatorPutState(
        TransactionState.COMMITTED, CoordinatorConflictException.class, snapshot);
    doReturn(Optional.empty()).when(coordinator).getState(anyId());
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context))
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(CoordinatorConflictException.class);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verifyCoordinatorPutState(TransactionState.COMMITTED, snapshot);
    verify(coordinator).getState(anyId());
    verify(handler).rollbackRecords(context);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @Test
  public void
      commit_CoordinatorConflictExceptionThrownInCoordinatorCommitAndThenExceptionThrownInGetState_ShouldThrowUnknown()
          throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    doThrowExceptionWhenCoordinatorPutState(
        TransactionState.COMMITTED, CoordinatorConflictException.class, snapshot);
    doThrow(CoordinatorException.class).when(coordinator).getState(anyId());
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verifyCoordinatorPutState(TransactionState.COMMITTED, snapshot);
    verify(coordinator).getState(anyId());
    verify(handler, never()).rollbackRecords(context);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @Test
  public void commit_ExceptionThrownInCoordinatorCommit_ShouldThrowUnknown()
      throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    doThrowExceptionWhenCoordinatorPutState(
        TransactionState.COMMITTED, CoordinatorException.class, snapshot);
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verifyCoordinatorPutState(TransactionState.COMMITTED, snapshot);
    verify(handler, never()).rollbackRecords(context);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @Test
  public void
      commit_BeforePreparationHookGiven_ShouldWaitBeforePreparationHookFinishesBeforeCommitState()
          throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    doThrowExceptionWhenCoordinatorPutState(
        TransactionState.COMMITTED, CoordinatorException.class, snapshot);
    // Lambda can't be spied...
    BeforePreparationHook delayedBeforePreparationHook =
        spy(
            new BeforePreparationHook() {
              @Override
              public Future<Void> handle(
                  TransactionTableMetadataManager tableMetadataManager,
                  TransactionContext context) {
                Uninterruptibles.sleepUninterruptibly(Duration.ofSeconds(2));
                return beforePreparationHookFuture;
              }
            });
    handler.setBeforePreparationHook(delayedBeforePreparationHook);
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    Instant start = Instant.now();
    assertThatThrownBy(() -> handler.commit(context))
        .isInstanceOf(UnknownTransactionStatusException.class);
    Instant end = Instant.now();

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verifyCoordinatorPutState(TransactionState.COMMITTED, snapshot);
    verify(handler, never()).rollbackRecords(context);
    verify(delayedBeforePreparationHook).handle(tableMetadataManager, context);
    // This means `commit()` waited until the callback was completed before throwing
    // an exception from `commitState()`.
    assertThat(Duration.between(start, end)).isGreaterThanOrEqualTo(Duration.ofSeconds(2));
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @Test
  public void commit_FailingBeforePreparationHookGiven_ShouldThrowCommitException()
      throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(new RuntimeException("Something is wrong"))
        .when(beforePreparationHook)
        .handle(any(), any());
    handler.setBeforePreparationHook(beforePreparationHook);
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    // Assert
    verify(storage, never()).mutate(anyList());
    verify(coordinator)
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.COMMITTED));
    verify(handler).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(any());
  }

  @Test
  public void commit_FailingBeforePreparationHookFutureGiven_ShouldThrowCommitException()
      throws ExecutionException, CoordinatorException, java.util.concurrent.ExecutionException,
          InterruptedException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    doThrow(new RuntimeException("Something is wrong")).when(beforePreparationHookFuture).get();
    setBeforePreparationHookIfNeeded(true);
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verify(coordinator)
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.COMMITTED));
    verify(handler).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void
      commit_FailingBeforePreparationHookGiven_InReadOnlyModeWithWriteOmissionEnabled_ShouldNotAbortStateNorRollbackRecords()
          throws ExecutionException, CoordinatorException, CrudException,
              UnknownTransactionStatusException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithoutWrites());
    doThrow(new RuntimeException("Something is wrong"))
        .when(beforePreparationHook)
        .handle(any(), any());
    handler.setBeforePreparationHook(beforePreparationHook);
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, true, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    // Assert
    // With coordinator write omission enabled, a read-only transaction must not write any
    // coordinator row even when the before-preparation hook fails.
    verify(storage, never()).mutate(anyList());
    verify(coordinator, never()).putState(any());
    verify(handler, never()).abortState(any());
    verify(handler, never()).rollbackRecords(any());
    verify(handler).onFailureBeforeCommit(any());
  }

  @Test
  public void
      commit_FailingBeforePreparationHookFutureGiven_InReadOnlyModeWithWriteOmissionEnabled_ShouldNotAbortStateNorRollbackRecords()
          throws ExecutionException, CoordinatorException, java.util.concurrent.ExecutionException,
              InterruptedException, UnknownTransactionStatusException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithoutWrites());
    doThrow(new RuntimeException("Something is wrong")).when(beforePreparationHookFuture).get();
    setBeforePreparationHookIfNeeded(true);
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, true, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    // Assert
    verify(coordinator, never()).putState(any());
    verify(handler, never()).abortState(any());
    verify(handler, never()).rollbackRecords(any());
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void
      commit_FailingBeforePreparationHookGiven_InReadOnlyModeWithWriteOmissionDisabled_ShouldAbortStateButNotRollbackRecords()
          throws ExecutionException, CoordinatorException, CrudException {
    // Arrange
    handler = spy(createCommitHandler(false));
    Snapshot snapshot = spy(prepareSnapshotWithoutWrites());
    doThrow(new RuntimeException("Something is wrong"))
        .when(beforePreparationHook)
        .handle(any(), any());
    handler.setBeforePreparationHook(beforePreparationHook);
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, true, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    // Assert
    // With coordinator write omission disabled, the ABORTED state is written even for a read-only
    // transaction, but record rollback is still skipped since there are no writes.
    verify(storage, never()).mutate(anyList());
    verify(coordinator)
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.ABORTED));
    verify(handler, never()).rollbackRecords(any());
    verify(handler).onFailureBeforeCommit(any());
  }

  @Test
  public void
      commit_FailingBeforePreparationHookFutureGiven_InReadOnlyModeWithWriteOmissionDisabled_ShouldAbortStateButNotRollbackRecords()
          throws ExecutionException, CoordinatorException, java.util.concurrent.ExecutionException,
              InterruptedException, UnknownTransactionStatusException {
    // Arrange
    handler = spy(createCommitHandler(false));
    Snapshot snapshot = spy(prepareSnapshotWithoutWrites());
    doThrow(new RuntimeException("Something is wrong")).when(beforePreparationHookFuture).get();
    setBeforePreparationHookIfNeeded(true);
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, true, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    // Assert
    // With coordinator write omission disabled, the ABORTED state is written even for a read-only
    // transaction, but record rollback is still skipped since there are no writes.
    verify(storage, never()).mutate(anyList());
    verify(coordinator)
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.ABORTED));
    verify(handler, never()).rollbackRecords(any());
    verify(handler).onFailureBeforeCommit(any());
  }

  @Test
  public void
      commit_FailingBeforePreparationHookGiven_InNonReadOnlyModeWithoutWritesAndWriteOmissionEnabled_ShouldNotAbortStateNorRollbackRecords()
          throws ExecutionException, CoordinatorException, UnknownTransactionStatusException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithoutWrites());
    doThrow(new RuntimeException("Something is wrong"))
        .when(beforePreparationHook)
        .handle(any(), any());
    handler.setBeforePreparationHook(beforePreparationHook);
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    // Assert
    // A non-read-only transaction without writes and deletes writes no coordinator row when
    // coordinator write omission is enabled, so the before-preparation hook failure must not abort
    // the state. There are also no records to roll back.
    verify(storage, never()).mutate(anyList());
    verify(coordinator, never()).putState(any());
    verify(handler, never()).abortState(any());
    verify(handler, never()).rollbackRecords(any());
    verify(handler).onFailureBeforeCommit(any());
  }

  @Test
  public void
      commit_FailingBeforePreparationHookFutureGiven_InNonReadOnlyModeWithoutWritesAndWriteOmissionEnabled_ShouldNotAbortStateNorRollbackRecords()
          throws CoordinatorException, java.util.concurrent.ExecutionException,
              InterruptedException, UnknownTransactionStatusException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithoutWrites());
    doThrow(new RuntimeException("Something is wrong")).when(beforePreparationHookFuture).get();
    setBeforePreparationHookIfNeeded(true);
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    // Assert
    // Same as above for the before-preparation hook future failure path.
    verify(coordinator, never()).putState(any());
    verify(handler, never()).abortState(any());
    verify(handler, never()).rollbackRecords(any());
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void canOnePhaseCommit_WhenOnePhaseCommitDisabled_ShouldReturnFalse() throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshot();
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    boolean actual = handler.canOnePhaseCommit(context);

    // Assert
    assertThat(actual).isFalse();
    verify(mutationsGrouper, never()).canBeGroupedAltogether(anyList());
  }

  @Test
  public void canOnePhaseCommit_WhenSerializableIsolationWithReads_ShouldReturnFalse()
      throws Exception {
    // Arrange
    CommitHandler handler = createCommitHandlerWithOnePhaseCommit();
    Snapshot snapshot = prepareSnapshot();

    Delete delete = prepareDelete();
    snapshot.putIntoDeleteSet(new Snapshot.Key(delete), delete);
    TransactionResult result = mock(TransactionResult.class);
    snapshot.putIntoReadSet(new Snapshot.Key(delete), Optional.of(result));

    // Add a read that is not in the write set or delete set to trigger validation
    Get get = prepareGet();
    snapshot.putIntoGetSet(get, Optional.empty());

    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act
    boolean actual = handler.canOnePhaseCommit(context);

    // Assert
    // With SERIALIZABLE isolation and reads that require validation, one-phase commit is not
    // allowed
    assertThat(actual).isFalse();
    verify(mutationsGrouper, never()).canBeGroupedAltogether(anyList());
  }

  @Test
  public void canOnePhaseCommit_WhenSnapshotIsolationWithReads_ShouldReturnTrue() throws Exception {
    // Arrange
    CommitHandler handler = createCommitHandlerWithOnePhaseCommit();
    Snapshot snapshot = prepareSnapshot();

    Delete delete = prepareDelete();
    snapshot.putIntoDeleteSet(new Snapshot.Key(delete), delete);
    TransactionResult result = mock(TransactionResult.class);
    snapshot.putIntoReadSet(new Snapshot.Key(delete), Optional.of(result));

    // Add a read that is not in the write set or delete set
    Get get = prepareGet();
    snapshot.putIntoGetSet(get, Optional.empty());

    doReturn(true).when(mutationsGrouper).canBeGroupedAltogether(anyList());

    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    boolean actual = handler.canOnePhaseCommit(context);

    // Assert
    // With SNAPSHOT isolation, validation is not required, so one-phase commit is allowed
    assertThat(actual).isTrue();
    verify(mutationsGrouper).canBeGroupedAltogether(anyList());
  }

  @Test
  public void canOnePhaseCommit_WhenNoWritesAndDeletes_ShouldReturnFalse() throws Exception {
    // Arrange
    CommitHandler handler = createCommitHandlerWithOnePhaseCommit();
    Snapshot snapshot = prepareSnapshotWithoutWrites();
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    boolean actual = handler.canOnePhaseCommit(context);

    // Assert
    assertThat(actual).isFalse();
    verify(mutationsGrouper, never()).canBeGroupedAltogether(anyList());
  }

  @Test
  public void canOnePhaseCommit_WhenDeleteWithoutExistingRecord_ShouldReturnFalse()
      throws Exception {
    // Arrange
    CommitHandler handler = createCommitHandlerWithOnePhaseCommit();
    Snapshot snapshot = prepareSnapshot();

    // Setup a delete with no corresponding record in read set
    Delete delete = prepareDelete();
    snapshot.putIntoDeleteSet(new Snapshot.Key(delete), delete);
    snapshot.putIntoReadSet(new Snapshot.Key(delete), Optional.empty());

    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    boolean actual = handler.canOnePhaseCommit(context);

    // Assert
    assertThat(actual).isFalse();
    verify(mutationsGrouper, never()).canBeGroupedAltogether(anyList());
  }

  @Test
  public void canOnePhaseCommit_WhenMutationsCanBeGrouped_ShouldReturnTrue() throws Exception {
    // Arrange
    CommitHandler handler = createCommitHandlerWithOnePhaseCommit();
    Snapshot snapshot = prepareSnapshot();

    Delete delete = prepareDelete();
    snapshot.putIntoDeleteSet(new Snapshot.Key(delete), delete);
    TransactionResult result = mock(TransactionResult.class);
    snapshot.putIntoReadSet(new Snapshot.Key(delete), Optional.of(result));

    doReturn(true).when(mutationsGrouper).canBeGroupedAltogether(anyList());

    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    boolean actual = handler.canOnePhaseCommit(context);

    // Assert
    assertThat(actual).isTrue();
    verify(mutationsGrouper).canBeGroupedAltogether(anyList());
  }

  @Test
  public void canOnePhaseCommit_WhenMutationsCannotBeGrouped_ShouldReturnFalse() throws Exception {
    // Arrange
    CommitHandler handler = createCommitHandlerWithOnePhaseCommit();
    Snapshot snapshot = prepareSnapshot();

    Delete delete = prepareDelete();
    snapshot.putIntoDeleteSet(new Snapshot.Key(delete), delete);
    TransactionResult result = mock(TransactionResult.class);
    snapshot.putIntoReadSet(new Snapshot.Key(delete), Optional.of(result));

    doReturn(false).when(mutationsGrouper).canBeGroupedAltogether(anyList());

    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    boolean actual = handler.canOnePhaseCommit(context);

    // Assert
    assertThat(actual).isFalse();
    verify(mutationsGrouper).canBeGroupedAltogether(anyList());
  }

  @Test
  public void
      canOnePhaseCommit_WhenMutationsGrouperThrowsExecutionException_ShouldThrowCommitException()
          throws ExecutionException {
    // Arrange
    CommitHandler handler = createCommitHandlerWithOnePhaseCommit();
    Snapshot snapshot = prepareSnapshot();

    Delete delete = prepareDelete();
    snapshot.putIntoDeleteSet(new Snapshot.Key(delete), delete);
    TransactionResult result = mock(TransactionResult.class);
    snapshot.putIntoReadSet(new Snapshot.Key(delete), Optional.of(result));

    doThrow(ExecutionException.class).when(mutationsGrouper).canBeGroupedAltogether(anyList());

    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.canOnePhaseCommit(context))
        .isInstanceOf(CommitException.class)
        .hasCauseInstanceOf(ExecutionException.class);
  }

  @Test
  public void onePhaseCommitRecords_WhenSuccessful_ShouldMutateUsingComposerMutations()
      throws CommitConflictException, UnknownTransactionStatusException, ExecutionException,
          CrudException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithSamePartitionPut());
    doNothing().when(storage).mutate(anyList());
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.onePhaseCommitRecords(context);

    // Assert
    verify(storage).mutate(anyList());
    verify(snapshot).to(any(OnePhaseCommitMutationComposer.class));
  }

  @Test
  public void
      onePhaseCommitRecords_WhenNoMutationExceptionThrown_ShouldThrowCommitConflictException()
          throws ExecutionException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithSamePartitionPut();
    doThrow(NoMutationException.class).when(storage).mutate(anyList());
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.onePhaseCommitRecords(context))
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(NoMutationException.class);
  }

  @Test
  public void
      onePhaseCommitRecords_WhenRetriableExecutionExceptionThrown_ShouldThrowCommitConflictException()
          throws ExecutionException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithSamePartitionPut();
    doThrow(RetriableExecutionException.class).when(storage).mutate(anyList());
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.onePhaseCommitRecords(context))
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(RetriableExecutionException.class);
  }

  @Test
  public void
      onePhaseCommitRecords_WhenExecutionExceptionThrown_ShouldThrowUnknownTransactionStatusException()
          throws ExecutionException, CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithSamePartitionPut();
    doThrow(ExecutionException.class).when(storage).mutate(anyList());
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.onePhaseCommitRecords(context))
        .isInstanceOf(UnknownTransactionStatusException.class)
        .hasCauseInstanceOf(ExecutionException.class);
  }

  @Test
  public void commit_OnePhaseCommitted_ShouldNotThrowAnyException()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    CommitHandler handler = spy(createCommitHandlerWithOnePhaseCommit());
    Snapshot snapshot = prepareSnapshotWithSamePartitionPut();

    doReturn(true).when(handler).canOnePhaseCommit(any(TransactionContext.class));
    doNothing().when(handler).onePhaseCommitRecords(any(TransactionContext.class));

    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, true, false);

    // Act
    handler.commit(context);

    // Assert
    verify(handler).canOnePhaseCommit(context);
    verify(handler).onePhaseCommitRecords(context);
  }

  @Test
  public void
      commit_OnePhaseCommitted_UnknownTransactionStatusExceptionThrown_ShouldThrowUnknownTransactionStatusException()
          throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    CommitHandler handler = spy(createCommitHandlerWithOnePhaseCommit());
    Snapshot snapshot = prepareSnapshotWithSamePartitionPut();

    doReturn(true).when(handler).canOnePhaseCommit(any(TransactionContext.class));
    doThrow(UnknownTransactionStatusException.class)
        .when(handler)
        .onePhaseCommitRecords(any(TransactionContext.class));

    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, true, false);

    // Act Assert
    assertThatThrownBy(() -> handler.commit(context))
        .isInstanceOf(UnknownTransactionStatusException.class);

    verify(handler).onFailureBeforeCommit(context);
  }

  protected void doThrowExceptionWhenCoordinatorPutState(
      TransactionState targetState, Class<? extends Exception> exceptionClass, Snapshot snapshot)
      throws CoordinatorException {
    doThrow(exceptionClass)
        .when(coordinator)
        .putState(
            new Coordinator.State(anyId(), expectedSingleGroupWriteSet(snapshot), targetState));
  }

  protected void doNothingWhenCoordinatorPutState() throws CoordinatorException {
    doNothing().when(coordinator).putState(any(Coordinator.State.class));
  }

  protected void verifyCoordinatorPutState(
      TransactionState expectedTransactionState, Snapshot snapshot) throws CoordinatorException {
    verify(coordinator)
        .putState(
            new Coordinator.State(
                anyId(), expectedSingleGroupWriteSet(snapshot), expectedTransactionState));
  }

  private void verifyBeforePreparationHook(
      boolean withBeforePreparationHook, TransactionContext context) {
    if (withBeforePreparationHook) {
      verify(beforePreparationHook).handle(eq(tableMetadataManager), eq(context));
    } else {
      verify(beforePreparationHook, never()).handle(any(), any());
    }
  }
}
