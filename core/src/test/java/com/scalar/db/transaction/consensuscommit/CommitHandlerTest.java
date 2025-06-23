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
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.Snapshot.ReadWriteSets;
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
  @Mock protected BeforePreparationSnapshotHook beforePreparationSnapshotHook;
  @Mock protected Future<Void> beforePreparationSnapshotHookFuture;

  private CommitHandler handler;
  protected ParallelExecutor parallelExecutor;
  protected MutationsGrouper mutationsGrouper;

  protected String anyId() {
    return ANY_ID;
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
                "storage1", StorageInfo.MutationAtomicityUnit.PARTITION, Integer.MAX_VALUE));
  }

  @AfterEach
  void tearDown() {
    extraCleanup();

    parallelExecutor.close();
  }

  private Put preparePut1() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Put(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME)
        .withValue(ANY_NAME_3, ANY_INT_1);
  }

  private Put preparePut2() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_3);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_4);
    return new Put(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME)
        .withValue(ANY_NAME_3, ANY_INT_2);
  }

  private Put preparePut3() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_3);
    return new Put(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME)
        .withValue(ANY_NAME_3, ANY_INT_2);
  }

  private Get prepareGet() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_3);
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

  private Snapshot prepareSnapshotWithDifferentPartitionPut() {
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

  private Snapshot prepareSnapshotWithSamePartitionPut() {
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

  private Snapshot prepareSnapshotWithoutWrites() {
    Snapshot snapshot = prepareSnapshot();

    Get get = prepareGet();
    snapshot.putIntoGetSet(get, Optional.empty());

    return snapshot;
  }

  private Snapshot prepareSnapshotWithoutReads() {
    Snapshot snapshot = prepareSnapshot();

    // same partition
    Put put1 = preparePut1();
    Put put3 = preparePut3();
    snapshot.putIntoWriteSet(new Snapshot.Key(put1), put1);
    snapshot.putIntoWriteSet(new Snapshot.Key(put3), put3);

    return snapshot;
  }

  private Snapshot prepareSnapshotWithIsolation(Isolation isolation) {
    return new Snapshot(anyId(), isolation, tableMetadataManager, new ParallelExecutor(config));
  }

  private Snapshot prepareSnapshot() {
    return new Snapshot(
        anyId(), Isolation.SNAPSHOT, tableMetadataManager, new ParallelExecutor(config));
  }

  private void setBeforePreparationSnapshotHookIfNeeded(boolean withSnapshotHook) {
    if (withSnapshotHook) {
      doReturn(beforePreparationSnapshotHookFuture)
          .when(beforePreparationSnapshotHook)
          .handle(any(), any());
      handler.setBeforePreparationSnapshotHook(beforePreparationSnapshotHook);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void commit_SnapshotWithDifferentPartitionPutsGiven_ShouldCommitRespectively(
      boolean withSnapshotHook)
      throws CommitException, UnknownTransactionStatusException, ExecutionException,
          CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    ReadWriteSets readWriteSets = snapshot.getReadWriteSets();
    doNothing().when(storage).mutate(anyList());
    doNothingWhenCoordinatorPutState();
    setBeforePreparationSnapshotHookIfNeeded(withSnapshotHook);

    // Act
    handler.commit(snapshot, false);

    // Assert
    verify(storage, times(4)).mutate(anyList());
    verify(snapshot).toSerializable(storage);
    verifyCoordinatorPutState(TransactionState.COMMITTED);
    verifySnapshotHook(withSnapshotHook, readWriteSets);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void commit_SnapshotWithSamePartitionPutsGiven_ShouldCommitAtOnce(boolean withSnapshotHook)
      throws CommitException, UnknownTransactionStatusException, ExecutionException,
          CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithSamePartitionPut());
    ReadWriteSets readWriteSets = snapshot.getReadWriteSets();
    doNothing().when(storage).mutate(anyList());
    doNothingWhenCoordinatorPutState();
    setBeforePreparationSnapshotHookIfNeeded(withSnapshotHook);

    // Act
    handler.commit(snapshot, false);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verify(snapshot).toSerializable(storage);
    verifyCoordinatorPutState(TransactionState.COMMITTED);
    verifySnapshotHook(withSnapshotHook, readWriteSets);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void commit_InReadOnlyMode_ShouldNotPrepareRecordsAndCommitStateAndCommitRecords(
      boolean withSnapshotHook)
      throws CommitException, UnknownTransactionStatusException, ExecutionException,
          CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithoutWrites());
    ReadWriteSets readWriteSets = snapshot.getReadWriteSets();
    setBeforePreparationSnapshotHookIfNeeded(withSnapshotHook);

    // Act
    handler.commit(snapshot, true);

    // Assert
    verify(storage, never()).mutate(anyList());
    verify(snapshot).toSerializable(storage);
    verify(coordinator, never()).putState(any());
    verifySnapshotHook(withSnapshotHook, readWriteSets);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void
      commit_NoWritesAndDeletesInSnapshot_ShouldNotPrepareRecordsAndCommitStateAndCommitRecords(
          boolean withSnapshotHook)
          throws CommitException, UnknownTransactionStatusException, ExecutionException,
              CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithoutWrites());
    ReadWriteSets readWriteSets = snapshot.getReadWriteSets();
    setBeforePreparationSnapshotHookIfNeeded(withSnapshotHook);

    // Act
    handler.commit(snapshot, false);

    // Assert
    verify(storage, never()).mutate(anyList());
    verify(snapshot).toSerializable(storage);
    verify(coordinator, never()).putState(any());
    verifySnapshotHook(withSnapshotHook, readWriteSets);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void
      commit_NoWritesAndDeletesInSnapshot_CoordinatorWriteOmissionOnReadOnlyDisabled_ShouldNotPrepareRecordsAndCommitRecordsButShouldCommitState(
          boolean withSnapshotHook)
          throws CommitException, UnknownTransactionStatusException, ExecutionException,
              CoordinatorException, ValidationConflictException {
    // Arrange
    handler = spy(createCommitHandler(false));
    Snapshot snapshot = spy(prepareSnapshotWithoutWrites());
    ReadWriteSets readWriteSets = snapshot.getReadWriteSets();
    setBeforePreparationSnapshotHookIfNeeded(withSnapshotHook);

    // Act
    handler.commit(snapshot, false);

    // Assert
    verify(storage, never()).mutate(anyList());
    verify(snapshot).toSerializable(storage);
    verifyCoordinatorPutState(TransactionState.COMMITTED);
    verifySnapshotHook(withSnapshotHook, readWriteSets);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void
      commit_NoWritesAndDeletesInSnapshot_ValidationFailed_ShouldNotPrepareRecordsAndAbortStateAndRollbackRecords(
          boolean withSnapshotHook)
          throws ExecutionException, CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithoutWrites());
    ReadWriteSets readWriteSets = snapshot.getReadWriteSets();
    setBeforePreparationSnapshotHookIfNeeded(withSnapshotHook);
    doThrow(ValidationConflictException.class).when(snapshot).toSerializable(storage);

    // Act Assert
    assertThatThrownBy(() -> handler.commit(snapshot, false))
        .isInstanceOf(CommitConflictException.class);

    // Assert
    verify(storage, never()).mutate(anyList());
    verify(snapshot).toSerializable(storage);
    verify(coordinator, never()).putState(any());
    verifySnapshotHook(withSnapshotHook, readWriteSets);
    verify(handler).onFailureBeforeCommit(any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void
      commit_NoWritesAndDeletesInSnapshot_ValidationFailed_CoordinatorWriteOmissionOnReadOnlyDisabled_ShouldNotPrepareRecordsAndRollbackRecordsButShouldAbortState(
          boolean withSnapshotHook)
          throws ExecutionException, CoordinatorException, ValidationConflictException {
    // Arrange
    handler = spy(createCommitHandler(false));
    Snapshot snapshot = spy(prepareSnapshotWithoutWrites());
    ReadWriteSets readWriteSets = snapshot.getReadWriteSets();
    setBeforePreparationSnapshotHookIfNeeded(withSnapshotHook);
    doThrow(ValidationConflictException.class).when(snapshot).toSerializable(storage);

    // Act Assert
    assertThatThrownBy(() -> handler.commit(snapshot, false))
        .isInstanceOf(CommitConflictException.class);

    // Assert
    verify(storage, never()).mutate(anyList());
    verify(snapshot).toSerializable(storage);
    verify(coordinator).putState(any());
    verifySnapshotHook(withSnapshotHook, readWriteSets);
    verify(handler).onFailureBeforeCommit(any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void commit_NoReadsInSnapshot_ShouldNotValidateRecords(boolean withSnapshotHook)
      throws CommitException, UnknownTransactionStatusException, ExecutionException,
          CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithoutReads());
    ReadWriteSets readWriteSets = snapshot.getReadWriteSets();
    doNothing().when(storage).mutate(anyList());
    doNothingWhenCoordinatorPutState();
    setBeforePreparationSnapshotHookIfNeeded(withSnapshotHook);

    // Act
    handler.commit(snapshot, false);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verify(snapshot, never()).toSerializable(storage);
    verifyCoordinatorPutState(TransactionState.COMMITTED);
    verifySnapshotHook(withSnapshotHook, readWriteSets);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @Test
  public void commit_NoMutationExceptionThrownInPrepareRecords_ShouldThrowCCException()
      throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(NoMutationException.class).when(storage).mutate(anyList());
    doNothing().when(coordinator).putState(any(Coordinator.State.class));
    doNothing().when(handler).rollbackRecords(any(Snapshot.class));

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot, false))
        .isInstanceOf(CommitConflictException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(handler).rollbackRecords(snapshot);
    verify(handler).onFailureBeforeCommit(snapshot);
  }

  @Test
  public void commit_RetriableExecutionExceptionThrownInPrepareRecords_ShouldThrowCCException()
      throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(RetriableExecutionException.class).when(storage).mutate(anyList());
    doNothing().when(coordinator).putState(any(Coordinator.State.class));
    doNothing().when(handler).rollbackRecords(any(Snapshot.class));

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot, false))
        .isInstanceOf(CommitConflictException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(handler).rollbackRecords(snapshot);
    verify(handler).onFailureBeforeCommit(snapshot);
  }

  @Test
  public void commit_ExceptionThrownInPrepareRecords_ShouldAbortAndRollbackRecords()
      throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(ExecutionException.class).when(storage).mutate(anyList());
    doNothing().when(coordinator).putState(any(Coordinator.State.class));
    doNothing().when(handler).rollbackRecords(any(Snapshot.class));

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot, false)).isInstanceOf(CommitException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(handler).rollbackRecords(snapshot);
    verify(handler).onFailureBeforeCommit(snapshot);
  }

  @Test
  public void
      commit_ExceptionThrownInPrepareRecordsAndCoordinatorConflictExceptionThrownInCoordinatorAbortThenAbortedReturnedInGetState_ShouldRollbackRecords()
          throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(ExecutionException.class).when(storage).mutate(anyList());
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    doReturn(Optional.of(new Coordinator.State(anyId(), TransactionState.ABORTED)))
        .when(coordinator)
        .getState(anyId());
    doNothing().when(handler).rollbackRecords(any(Snapshot.class));

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot, false)).isInstanceOf(CommitException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(coordinator).getState(anyId());
    verify(handler).rollbackRecords(snapshot);
    verify(handler).onFailureBeforeCommit(snapshot);
  }

  @Test
  public void
      commit_ExceptionThrownInPrepareRecordsAndCoordinatorConflictExceptionThrownInCoordinatorAbortThenNothingReturnedInGetState_ShouldThrowUnknown()
          throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(ExecutionException.class).when(storage).mutate(anyList());
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    doReturn(Optional.empty()).when(coordinator).getState(anyId());

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot, false))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(coordinator).getState(anyId());
    verify(handler, never()).rollbackRecords(snapshot);
    verify(handler).onFailureBeforeCommit(snapshot);
  }

  @Test
  public void
      commit_ExceptionThrownInPrepareRecordsAndCoordinatorConflictExceptionThrownInCoordinatorAbortThenExceptionThrownInGetState_ShouldThrowUnknown()
          throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(ExecutionException.class).when(storage).mutate(anyList());
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    doThrow(CoordinatorException.class).when(coordinator).getState(anyId());

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot, false))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(coordinator).getState(anyId());
    verify(handler, never()).rollbackRecords(snapshot);
    verify(handler).onFailureBeforeCommit(snapshot);
  }

  @Test
  public void
      commit_ExceptionThrownInPrepareRecordsAndCoordinatorExceptionThrownInCoordinatorAbort_ShouldThrowUnknown()
          throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(ExecutionException.class).when(storage).mutate(anyList());
    doThrow(CoordinatorException.class)
        .when(coordinator)
        .putState(new Coordinator.State(anyId(), TransactionState.ABORTED));

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot, false))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(handler, never()).rollbackRecords(snapshot);
    verify(handler).onFailureBeforeCommit(snapshot);
  }

  @Test
  public void commit_ValidationConflictExceptionThrownInValidation_ShouldAbortAndRollbackRecords()
      throws ExecutionException, CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(storage).mutate(anyList());
    doThrow(ValidationConflictException.class).when(snapshot).toSerializable(storage);
    doNothing().when(coordinator).putState(any(Coordinator.State.class));
    doNothing().when(handler).rollbackRecords(any(Snapshot.class));

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot, false)).isInstanceOf(CommitException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(handler).rollbackRecords(snapshot);
    verify(handler).onFailureBeforeCommit(snapshot);
  }

  @Test
  public void commit_ExceptionThrownInValidation_ShouldAbortAndRollbackRecords()
      throws ExecutionException, CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(storage).mutate(anyList());
    doThrow(ExecutionException.class).when(snapshot).toSerializable(storage);
    doNothing().when(coordinator).putState(any(Coordinator.State.class));
    doNothing().when(handler).rollbackRecords(any(Snapshot.class));

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot, false)).isInstanceOf(CommitException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(handler).rollbackRecords(snapshot);
    verify(handler).onFailureBeforeCommit(snapshot);
  }

  @Test
  public void
      commit_ExceptionThrownInValidationAndCoordinatorConflictExceptionThrownInCoordinatorAbortThenAbortedReturnedInGetState_ShouldRollbackRecords()
          throws ExecutionException, CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(storage).mutate(anyList());
    doThrow(ExecutionException.class).when(snapshot).toSerializable(storage);
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    doReturn(Optional.of(new Coordinator.State(anyId(), TransactionState.ABORTED)))
        .when(coordinator)
        .getState(anyId());
    doNothing().when(handler).rollbackRecords(any(Snapshot.class));

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot, false)).isInstanceOf(CommitException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(coordinator).getState(anyId());
    verify(handler).rollbackRecords(snapshot);
    verify(handler).onFailureBeforeCommit(snapshot);
  }

  @Test
  public void
      commit_ExceptionThrownInValidationAndCoordinatorConflictExceptionThrownInCoordinatorAbortThenNothingReturnedInGetState_ShouldThrowUnknown()
          throws ExecutionException, CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(storage).mutate(anyList());
    doThrow(ExecutionException.class).when(snapshot).toSerializable(storage);
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    doReturn(Optional.empty()).when(coordinator).getState(anyId());

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot, false))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(coordinator).getState(anyId());
    verify(handler, never()).rollbackRecords(snapshot);
    verify(handler).onFailureBeforeCommit(snapshot);
  }

  @Test
  public void
      commit_ExceptionThrownInValidationAndCoordinatorConflictExceptionThrownInCoordinatorAbortThenExceptionThrownInGetState_ShouldThrowUnknown()
          throws ExecutionException, CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(storage).mutate(anyList());
    doThrow(ExecutionException.class).when(snapshot).toSerializable(storage);
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    doThrow(CoordinatorException.class).when(coordinator).getState(anyId());

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot, false))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(coordinator).getState(anyId());
    verify(handler, never()).rollbackRecords(snapshot);
    verify(handler).onFailureBeforeCommit(snapshot);
  }

  @Test
  public void
      commit_ExceptionThrownInValidationAndCoordinatorExceptionThrownInCoordinatorAbort_ShouldThrowUnknown()
          throws ExecutionException, CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(storage).mutate(anyList());
    doThrow(ExecutionException.class).when(snapshot).toSerializable(storage);
    doThrow(CoordinatorException.class)
        .when(coordinator)
        .putState(new Coordinator.State(anyId(), TransactionState.ABORTED));

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot, false))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(handler, never()).rollbackRecords(snapshot);
    verify(handler).onFailureBeforeCommit(snapshot);
  }

  @Test
  public void
      commit_CoordinatorConflictExceptionThrownInCoordinatorCommitAndThenCommittedReturnedInGetState_ShouldBeIgnored()
          throws ExecutionException, CoordinatorException, CommitException,
              UnknownTransactionStatusException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    doThrowExceptionWhenCoordinatorPutState(
        TransactionState.COMMITTED, CoordinatorConflictException.class);
    doReturn(Optional.of(new Coordinator.State(anyId(), TransactionState.COMMITTED)))
        .when(coordinator)
        .getState(anyId());

    // Act
    handler.commit(snapshot, false);

    // Assert
    verify(storage, times(4)).mutate(anyList());
    verifyCoordinatorPutState(TransactionState.COMMITTED);
    verify(coordinator).getState(anyId());
    verify(handler, never()).rollbackRecords(snapshot);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @Test
  public void
      commit_CoordinatorConflictExceptionThrownInCoordinatorCommitAndThenAbortedReturnedInGetState_ShouldRollbackRecords()
          throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    doThrowExceptionWhenCoordinatorPutState(
        TransactionState.COMMITTED, CoordinatorConflictException.class);
    doReturn(Optional.of(new Coordinator.State(anyId(), TransactionState.ABORTED)))
        .when(coordinator)
        .getState(anyId());

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot, false)).isInstanceOf(CommitException.class);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verifyCoordinatorPutState(TransactionState.COMMITTED);
    verify(coordinator).getState(anyId());
    verify(handler).rollbackRecords(snapshot);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @Test
  public void
      commit_CoordinatorConflictExceptionThrownInCoordinatorCommitAndThenNothingReturnedInGetState_ShouldThrowUnknown()
          throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    doThrowExceptionWhenCoordinatorPutState(
        TransactionState.COMMITTED, CoordinatorConflictException.class);
    doReturn(Optional.empty()).when(coordinator).getState(anyId());

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot, false))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verifyCoordinatorPutState(TransactionState.COMMITTED);
    verify(coordinator).getState(anyId());
    verify(handler, never()).rollbackRecords(snapshot);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @Test
  public void
      commit_CoordinatorConflictExceptionThrownInCoordinatorCommitAndThenExceptionThrownInGetState_ShouldThrowUnknown()
          throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    doThrowExceptionWhenCoordinatorPutState(
        TransactionState.COMMITTED, CoordinatorConflictException.class);
    doThrow(CoordinatorException.class).when(coordinator).getState(anyId());

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot, false))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verifyCoordinatorPutState(TransactionState.COMMITTED);
    verify(coordinator).getState(anyId());
    verify(handler, never()).rollbackRecords(snapshot);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @Test
  public void commit_ExceptionThrownInCoordinatorCommit_ShouldThrowUnknown()
      throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    doThrowExceptionWhenCoordinatorPutState(TransactionState.COMMITTED, CoordinatorException.class);

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot, false))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verifyCoordinatorPutState(TransactionState.COMMITTED);
    verify(handler, never()).rollbackRecords(snapshot);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @Test
  public void commit_SnapshotHookGiven_ShouldWaitSnapshotHookFinishesBeforeCommitState()
      throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    ReadWriteSets readWriteSets = snapshot.getReadWriteSets();
    doNothing().when(storage).mutate(anyList());
    doThrowExceptionWhenCoordinatorPutState(TransactionState.COMMITTED, CoordinatorException.class);
    // Lambda can't be spied...
    BeforePreparationSnapshotHook delayedBeforePreparationSnapshotHook =
        spy(
            new BeforePreparationSnapshotHook() {
              @Override
              public Future<Void> handle(
                  TransactionTableMetadataManager tableMetadataManager,
                  Snapshot.ReadWriteSets readWriteSets) {
                Uninterruptibles.sleepUninterruptibly(Duration.ofSeconds(2));
                return beforePreparationSnapshotHookFuture;
              }
            });
    handler.setBeforePreparationSnapshotHook(delayedBeforePreparationSnapshotHook);

    // Act
    Instant start = Instant.now();
    assertThatThrownBy(() -> handler.commit(snapshot, false))
        .isInstanceOf(UnknownTransactionStatusException.class);
    Instant end = Instant.now();

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verifyCoordinatorPutState(TransactionState.COMMITTED);
    verify(handler, never()).rollbackRecords(snapshot);
    verify(delayedBeforePreparationSnapshotHook).handle(tableMetadataManager, readWriteSets);
    // This means `commit()` waited until the callback was completed before throwing
    // an exception from `commitState()`.
    assertThat(Duration.between(start, end)).isGreaterThanOrEqualTo(Duration.ofSeconds(2));
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @Test
  public void commit_FailingSnapshotHookGiven_ShouldThrowCommitException()
      throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doThrow(new RuntimeException("Something is wrong"))
        .when(beforePreparationSnapshotHook)
        .handle(any(), any());
    handler.setBeforePreparationSnapshotHook(beforePreparationSnapshotHook);

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot, false)).isInstanceOf(CommitException.class);

    // Assert
    verify(storage, never()).mutate(anyList());
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(handler).rollbackRecords(snapshot);
    verify(handler).onFailureBeforeCommit(any());
  }

  @Test
  public void commit_FailingSnapshotHookFutureGiven_ShouldThrowCommitException()
      throws ExecutionException, CoordinatorException, java.util.concurrent.ExecutionException,
          InterruptedException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    doThrow(new RuntimeException("Something is wrong"))
        .when(beforePreparationSnapshotHookFuture)
        .get();
    setBeforePreparationSnapshotHookIfNeeded(true);

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot, false)).isInstanceOf(CommitException.class);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(handler).rollbackRecords(snapshot);
    verify(handler).onFailureBeforeCommit(snapshot);
  }

  @Test
  public void canOnePhaseCommit_WhenOnePhaseCommitDisabled_ShouldReturnFalse() throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshot();

    // Act
    boolean actual = handler.canOnePhaseCommit(snapshot);

    // Assert
    assertThat(actual).isFalse();
    verify(mutationsGrouper, never()).canBeGroupedAltogether(anyList());
  }

  @Test
  public void canOnePhaseCommit_WhenValidationRequired_ShouldReturnFalse() throws Exception {
    // Arrange
    CommitHandler handler = createCommitHandlerWithOnePhaseCommit();
    Snapshot snapshot = prepareSnapshotWithIsolation(Isolation.SERIALIZABLE);

    // Act
    boolean actual = handler.canOnePhaseCommit(snapshot);

    // Assert
    assertThat(actual).isFalse();
    verify(mutationsGrouper, never()).canBeGroupedAltogether(anyList());
  }

  @Test
  public void canOnePhaseCommit_WhenNoWritesAndDeletes_ShouldReturnFalse() throws Exception {
    // Arrange
    CommitHandler handler = createCommitHandlerWithOnePhaseCommit();
    Snapshot snapshot = prepareSnapshotWithoutWrites();

    // Act
    boolean actual = handler.canOnePhaseCommit(snapshot);

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

    // Act
    boolean actual = handler.canOnePhaseCommit(snapshot);

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

    // Act
    boolean actual = handler.canOnePhaseCommit(snapshot);

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

    // Act
    boolean actual = handler.canOnePhaseCommit(snapshot);

    // Assert
    assertThat(actual).isFalse();
    verify(mutationsGrouper).canBeGroupedAltogether(anyList());
  }

  @Test
  public void canOnePhaseCommit_WhenMutationsGrouperThrowsException_ShouldThrowCommitException()
      throws ExecutionException {
    // Arrange
    CommitHandler handler = createCommitHandlerWithOnePhaseCommit();
    Snapshot snapshot = prepareSnapshot();

    Delete delete = prepareDelete();
    snapshot.putIntoDeleteSet(new Snapshot.Key(delete), delete);
    TransactionResult result = mock(TransactionResult.class);
    snapshot.putIntoReadSet(new Snapshot.Key(delete), Optional.of(result));

    doThrow(ExecutionException.class).when(mutationsGrouper).canBeGroupedAltogether(anyList());

    // Act Assert
    assertThatThrownBy(() -> handler.canOnePhaseCommit(snapshot))
        .isInstanceOf(CommitException.class)
        .hasCauseInstanceOf(ExecutionException.class);
  }

  @Test
  public void onePhaseCommitRecords_WhenSuccessful_ShouldMutateUsingComposerMutations()
      throws CommitException, ExecutionException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithSamePartitionPut());
    doNothing().when(storage).mutate(anyList());

    // Act
    handler.onePhaseCommitRecords(snapshot);

    // Assert
    verify(storage).mutate(anyList());
    verify(snapshot).to(any(OnePhaseCommitMutationComposer.class));
  }

  @Test
  public void
      onePhaseCommitRecords_WhenNoMutationExceptionThrown_ShouldThrowCommitConflictException()
          throws ExecutionException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithSamePartitionPut();
    doThrow(NoMutationException.class).when(storage).mutate(anyList());

    // Act Assert
    assertThatThrownBy(() -> handler.onePhaseCommitRecords(snapshot))
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(NoMutationException.class);
  }

  @Test
  public void
      onePhaseCommitRecords_WhenRetriableExecutionExceptionThrown_ShouldThrowCommitConflictException()
          throws ExecutionException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithSamePartitionPut();
    doThrow(RetriableExecutionException.class).when(storage).mutate(anyList());

    // Act Assert
    assertThatThrownBy(() -> handler.onePhaseCommitRecords(snapshot))
        .isInstanceOf(CommitConflictException.class)
        .hasCauseInstanceOf(RetriableExecutionException.class);
  }

  @Test
  public void onePhaseCommitRecords_WhenExecutionExceptionThrown_ShouldThrowCommitException()
      throws ExecutionException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithSamePartitionPut();
    doThrow(ExecutionException.class).when(storage).mutate(anyList());

    // Act Assert
    assertThatThrownBy(() -> handler.onePhaseCommitRecords(snapshot))
        .isInstanceOf(CommitException.class)
        .hasCauseInstanceOf(ExecutionException.class);
  }

  @Test
  public void commit_OnePhaseCommitted_ShouldNotThrowAnyException()
      throws CommitException, UnknownTransactionStatusException {
    // Arrange
    CommitHandler handler = spy(createCommitHandlerWithOnePhaseCommit());
    Snapshot snapshot = prepareSnapshotWithSamePartitionPut();

    doReturn(true).when(handler).canOnePhaseCommit(snapshot);
    doNothing().when(handler).onePhaseCommitRecords(snapshot);

    // Act
    handler.commit(snapshot, true);

    // Assert
    verify(handler).canOnePhaseCommit(snapshot);
    verify(handler).onePhaseCommitRecords(snapshot);
  }

  @Test
  public void commit_OnePhaseCommitted_CommitExceptionThrown_ShouldThrowCommitException()
      throws CommitException {
    // Arrange
    CommitHandler handler = spy(createCommitHandlerWithOnePhaseCommit());
    Snapshot snapshot = prepareSnapshotWithSamePartitionPut();

    doReturn(true).when(handler).canOnePhaseCommit(snapshot);
    doThrow(CommitException.class).when(handler).onePhaseCommitRecords(snapshot);

    // Act Assert
    assertThatThrownBy(() -> handler.commit(snapshot, true)).isInstanceOf(CommitException.class);

    verify(handler).onFailureBeforeCommit(snapshot);
  }

  private CommitHandler createCommitHandlerWithOnePhaseCommit() {
    return new CommitHandler(
        storage, coordinator, tableMetadataManager, parallelExecutor, mutationsGrouper, true, true);
  }

  protected void doThrowExceptionWhenCoordinatorPutState(
      TransactionState targetState, Class<? extends Exception> exceptionClass)
      throws CoordinatorException {
    doThrow(exceptionClass).when(coordinator).putState(new Coordinator.State(anyId(), targetState));
  }

  protected void doNothingWhenCoordinatorPutState() throws CoordinatorException {
    doNothing().when(coordinator).putState(any(Coordinator.State.class));
  }

  protected void verifyCoordinatorPutState(TransactionState expectedTransactionState)
      throws CoordinatorException {
    verify(coordinator).putState(new Coordinator.State(anyId(), expectedTransactionState));
  }

  private void verifySnapshotHook(boolean withSnapshotHook, Snapshot.ReadWriteSets readWriteSets) {
    if (withSnapshotHook) {
      verify(beforePreparationSnapshotHook).handle(eq(tableMetadataManager), eq(readWriteSets));
    } else {
      verify(beforePreparationSnapshotHook, never()).handle(any(), any());
    }
  }
}
