package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Put;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.BeforePreparationSnapshotHook.BeforePreparationSnapshotHookFuture;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
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
  @Mock protected ConsensusCommitConfig config;
  @Mock protected BeforePreparationSnapshotHook beforePreparationSnapshotHook;
  @Mock protected BeforePreparationSnapshotHookFuture beforePreparationSnapshotHookFuture;

  private CommitHandler handler;
  protected ParallelExecutor parallelExecutor;

  protected String anyId() {
    return ANY_ID;
  }

  protected void extraInitialize() {}

  protected void extraCleanup() {}

  protected CommitHandler createCommitHandler() {
    return new CommitHandler(storage, coordinator, tableMetadataManager, parallelExecutor);
  }

  @BeforeEach
  void setUp() throws Exception {
    parallelExecutor = new ParallelExecutor(config);
    handler = spy(createCommitHandler());

    extraInitialize();
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

  private Snapshot prepareSnapshotWithDifferentPartitionPut() {
    Snapshot snapshot =
        new Snapshot(
            anyId(),
            Isolation.SNAPSHOT,
            SerializableStrategy.EXTRA_WRITE,
            tableMetadataManager,
            new ParallelExecutor(config));

    // different partition
    Put put1 = preparePut1();
    Put put2 = preparePut2();
    snapshot.put(new Snapshot.Key(put1), put1);
    snapshot.put(new Snapshot.Key(put2), put2);

    return snapshot;
  }

  private Snapshot prepareSnapshotWithSamePartitionPut() {
    Snapshot snapshot =
        new Snapshot(
            anyId(),
            Isolation.SNAPSHOT,
            SerializableStrategy.EXTRA_WRITE,
            tableMetadataManager,
            new ParallelExecutor(config));

    // same partition
    Put put1 = preparePut1();
    Put put3 = preparePut3();
    snapshot.put(new Snapshot.Key(put1), put1);
    snapshot.put(new Snapshot.Key(put3), put3);

    return snapshot;
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
          CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    doNothingWhenCoordinatorPutState();
    setBeforePreparationSnapshotHookIfNeeded(withSnapshotHook);

    // Act
    handler.commit(snapshot);

    // Assert
    verify(storage, times(4)).mutate(anyList());
    verifyCoordinatorPutState(TransactionState.COMMITTED);
    verifySnapshotHook(withSnapshotHook, snapshot);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void commit_SnapshotWithSamePartitionPutsGiven_ShouldCommitAtOnce(boolean withSnapshotHook)
      throws CommitException, UnknownTransactionStatusException, ExecutionException,
          CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithSamePartitionPut();
    doNothing().when(storage).mutate(anyList());
    doNothingWhenCoordinatorPutState();
    setBeforePreparationSnapshotHookIfNeeded(withSnapshotHook);

    // Act
    handler.commit(snapshot);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verifyCoordinatorPutState(TransactionState.COMMITTED);
    verifySnapshotHook(withSnapshotHook, snapshot);
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
    assertThatThrownBy(() -> handler.commit(snapshot)).isInstanceOf(CommitConflictException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(handler).rollbackRecords(snapshot);
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
    assertThatThrownBy(() -> handler.commit(snapshot)).isInstanceOf(CommitConflictException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(handler).rollbackRecords(snapshot);
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
    assertThatThrownBy(() -> handler.commit(snapshot)).isInstanceOf(CommitException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(handler).rollbackRecords(snapshot);
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
    assertThatThrownBy(() -> handler.commit(snapshot)).isInstanceOf(CommitException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(coordinator).getState(anyId());
    verify(handler).rollbackRecords(snapshot);
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
    assertThatThrownBy(() -> handler.commit(snapshot))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(coordinator).getState(anyId());
    verify(handler, never()).rollbackRecords(snapshot);
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
    assertThatThrownBy(() -> handler.commit(snapshot))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(coordinator).getState(anyId());
    verify(handler, never()).rollbackRecords(snapshot);
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
    assertThatThrownBy(() -> handler.commit(snapshot))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(handler, never()).rollbackRecords(snapshot);
  }

  @Test
  public void commit_ValidationConflictExceptionThrownInValidation_ShouldAbortAndRollbackRecords()
      throws ExecutionException, CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(storage).mutate(anyList());
    doThrow(ValidationConflictException.class).when(snapshot).toSerializableWithExtraRead(storage);
    doNothing().when(coordinator).putState(any(Coordinator.State.class));
    doNothing().when(handler).rollbackRecords(any(Snapshot.class));

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot)).isInstanceOf(CommitException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(handler).rollbackRecords(snapshot);
  }

  @Test
  public void commit_ExceptionThrownInValidation_ShouldAbortAndRollbackRecords()
      throws ExecutionException, CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(storage).mutate(anyList());
    doThrow(ExecutionException.class).when(snapshot).toSerializableWithExtraRead(storage);
    doNothing().when(coordinator).putState(any(Coordinator.State.class));
    doNothing().when(handler).rollbackRecords(any(Snapshot.class));

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot)).isInstanceOf(CommitException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(handler).rollbackRecords(snapshot);
  }

  @Test
  public void
      commit_ExceptionThrownInValidationAndCoordinatorConflictExceptionThrownInCoordinatorAbortThenAbortedReturnedInGetState_ShouldRollbackRecords()
          throws ExecutionException, CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(storage).mutate(anyList());
    doThrow(ExecutionException.class).when(snapshot).toSerializableWithExtraRead(storage);
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    doReturn(Optional.of(new Coordinator.State(anyId(), TransactionState.ABORTED)))
        .when(coordinator)
        .getState(anyId());
    doNothing().when(handler).rollbackRecords(any(Snapshot.class));

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot)).isInstanceOf(CommitException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(coordinator).getState(anyId());
    verify(handler).rollbackRecords(snapshot);
  }

  @Test
  public void
      commit_ExceptionThrownInValidationAndCoordinatorConflictExceptionThrownInCoordinatorAbortThenNothingReturnedInGetState_ShouldThrowUnknown()
          throws ExecutionException, CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(storage).mutate(anyList());
    doThrow(ExecutionException.class).when(snapshot).toSerializableWithExtraRead(storage);
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    doReturn(Optional.empty()).when(coordinator).getState(anyId());

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(coordinator).getState(anyId());
    verify(handler, never()).rollbackRecords(snapshot);
  }

  @Test
  public void
      commit_ExceptionThrownInValidationAndCoordinatorConflictExceptionThrownInCoordinatorAbortThenExceptionThrownInGetState_ShouldThrowUnknown()
          throws ExecutionException, CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(storage).mutate(anyList());
    doThrow(ExecutionException.class).when(snapshot).toSerializableWithExtraRead(storage);
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    doThrow(CoordinatorException.class).when(coordinator).getState(anyId());

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(coordinator).getState(anyId());
    verify(handler, never()).rollbackRecords(snapshot);
  }

  @Test
  public void
      commit_ExceptionThrownInValidationAndCoordinatorExceptionThrownInCoordinatorAbort_ShouldThrowUnknown()
          throws ExecutionException, CoordinatorException, ValidationConflictException {
    // Arrange
    Snapshot snapshot = spy(prepareSnapshotWithDifferentPartitionPut());
    doNothing().when(storage).mutate(anyList());
    doThrow(ExecutionException.class).when(snapshot).toSerializableWithExtraRead(storage);
    doThrow(CoordinatorException.class)
        .when(coordinator)
        .putState(new Coordinator.State(anyId(), TransactionState.ABORTED));

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert

    // An exception is thrown before group commit even when it's enabled. So, the normal
    // `putState(ABORT)` must be called.
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(handler, never()).rollbackRecords(snapshot);
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
    handler.commit(snapshot);

    // Assert
    verify(storage, times(4)).mutate(anyList());
    verifyCoordinatorPutState(TransactionState.COMMITTED);
    verify(coordinator).getState(anyId());
    verify(handler, never()).rollbackRecords(snapshot);
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
    assertThatThrownBy(() -> handler.commit(snapshot)).isInstanceOf(CommitException.class);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verifyCoordinatorPutState(TransactionState.COMMITTED);
    verify(coordinator).getState(anyId());
    verify(handler).rollbackRecords(snapshot);
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
    assertThatThrownBy(() -> handler.commit(snapshot))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verifyCoordinatorPutState(TransactionState.COMMITTED);
    verify(coordinator).getState(anyId());
    verify(handler, never()).rollbackRecords(snapshot);
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
    assertThatThrownBy(() -> handler.commit(snapshot))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verifyCoordinatorPutState(TransactionState.COMMITTED);
    verify(coordinator).getState(anyId());
    verify(handler, never()).rollbackRecords(snapshot);
  }

  @Test
  public void commit_ExceptionThrownInCoordinatorCommit_ShouldThrowUnknown()
      throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    doThrowExceptionWhenCoordinatorPutState(TransactionState.COMMITTED, CoordinatorException.class);

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verifyCoordinatorPutState(TransactionState.COMMITTED);
    verify(handler, never()).rollbackRecords(snapshot);
  }

  @Test
  public void commit_SnapshotHookGiven_ShouldWaitSnapshotHookFinishesBeforeCommitState()
      throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    doThrowExceptionWhenCoordinatorPutState(TransactionState.COMMITTED, CoordinatorException.class);
    // Lambda can't be spied...
    BeforePreparationSnapshotHook delayedBeforePreparationSnapshotHook =
        spy(
            new BeforePreparationSnapshotHook() {
              @Override
              public BeforePreparationSnapshotHookFuture handle(
                  TransactionTableMetadataManager tableMetadataManager, Snapshot ss) {
                Uninterruptibles.sleepUninterruptibly(Duration.ofSeconds(2));
                return beforePreparationSnapshotHookFuture;
              }
            });
    handler.setBeforePreparationSnapshotHook(delayedBeforePreparationSnapshotHook);

    // Act
    Instant start = Instant.now();
    assertThatThrownBy(() -> handler.commit(snapshot))
        .isInstanceOf(UnknownTransactionStatusException.class);
    Instant end = Instant.now();

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verifyCoordinatorPutState(TransactionState.COMMITTED);
    verify(handler, never()).rollbackRecords(snapshot);
    verify(delayedBeforePreparationSnapshotHook).handle(tableMetadataManager, snapshot);
    // This means `commit()` waited until the callback was completed before throwing
    // an exception from `commitState()`.
    assertThat(Duration.between(start, end)).isGreaterThanOrEqualTo(Duration.ofSeconds(2));
  }

  @Test
  public void commit_FailingSnapshotHookGiven_ShouldThrowCommitException()
      throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothingWhenCoordinatorPutState();
    doThrow(new RuntimeException("Something is wrong"))
        .when(beforePreparationSnapshotHook)
        .handle(any(), any());
    handler.setBeforePreparationSnapshotHook(beforePreparationSnapshotHook);

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot)).isInstanceOf(CommitException.class);

    // Assert
    verify(storage, never()).mutate(anyList());
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(handler).rollbackRecords(snapshot);
  }

  @Test
  public void commit_FailingSnapshotHookFutureGiven_ShouldThrowCommitException()
      throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    doNothingWhenCoordinatorPutState();
    // Lambda can't be spied...
    BeforePreparationSnapshotHook failingBeforePreparationSnapshotHook =
        spy(
            new BeforePreparationSnapshotHook() {
              @Override
              public BeforePreparationSnapshotHookFuture handle(
                  TransactionTableMetadataManager tableMetadataManager, Snapshot ss) {
                return () -> {
                  throw new RuntimeException("Something is wrong");
                };
              }
            });
    handler.setBeforePreparationSnapshotHook(failingBeforePreparationSnapshotHook);

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot)).isInstanceOf(CommitException.class);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verify(coordinator).putState(new Coordinator.State(anyId(), TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(anyId(), TransactionState.COMMITTED));
    verify(handler).rollbackRecords(snapshot);
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

  private void verifySnapshotHook(boolean withSnapshotHook, Snapshot snapshot) {
    if (withSnapshotHook) {
      verify(beforePreparationSnapshotHook).handle(eq(tableMetadataManager), eq(snapshot));
    } else {
      verify(beforePreparationSnapshotHook, never()).handle(any(), any());
    }
  }
}
