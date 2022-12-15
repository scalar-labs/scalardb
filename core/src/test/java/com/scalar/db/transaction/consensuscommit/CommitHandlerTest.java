package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Put;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.io.Key;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CommitHandlerTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_ID = "id";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final String ANY_TEXT_4 = "text4";
  private static final int ANY_INT_1 = 100;
  private static final int ANY_INT_2 = 200;

  @Mock private DistributedStorage storage;
  @Mock private Coordinator coordinator;
  @Mock private TransactionTableMetadataManager tableMetadataManager;
  @Mock private ConsensusCommitConfig config;

  private CommitHandler handler;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    handler =
        spy(
            new CommitHandler(
                storage, coordinator, tableMetadataManager, new ParallelExecutor(config)));
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
            ANY_ID,
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
            ANY_ID,
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

  @Test
  public void commit_SnapshotWithDifferentPartitionPutsGiven_ShouldCommitRespectively()
      throws CommitException, UnknownTransactionStatusException, ExecutionException,
          CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    doNothing().when(storage).mutate(anyList());
    doNothing().when(coordinator).putState(any(Coordinator.State.class));

    // Act
    handler.commit(snapshot);

    // Assert
    verify(storage, times(4)).mutate(anyList());
    verify(coordinator).putState(new Coordinator.State(ANY_ID, TransactionState.COMMITTED));
  }

  @Test
  public void commit_SnapshotWithSamePartitionPutsGiven_ShouldCommitAtOnce()
      throws CommitException, UnknownTransactionStatusException, ExecutionException,
          CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithSamePartitionPut();
    doNothing().when(storage).mutate(anyList());
    doNothing().when(coordinator).putState(any(Coordinator.State.class));

    // Act
    handler.commit(snapshot);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verify(coordinator).putState(new Coordinator.State(ANY_ID, TransactionState.COMMITTED));
  }

  @Test
  public void commit_NoMutationExceptionThrownInPrepareRecords_ShouldThrowCCException()
      throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    ExecutionException toThrow = mock(NoMutationException.class);
    doThrow(toThrow).when(storage).mutate(anyList());
    doNothing().when(coordinator).putState(any(Coordinator.State.class));
    doNothing().when(handler).rollbackRecords(any(Snapshot.class));

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot))
        .isInstanceOf(CommitConflictException.class)
        .hasCause(toThrow);

    // Assert
    verify(coordinator).putState(new Coordinator.State(ANY_ID, TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(ANY_ID, TransactionState.COMMITTED));
    verify(handler).rollbackRecords(snapshot);
  }

  @Test
  public void commit_RetriableExecutionExceptionThrownInPrepareRecords_ShouldThrowCCException()
      throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    ExecutionException toThrow = mock(RetriableExecutionException.class);
    doThrow(toThrow).when(storage).mutate(anyList());
    doNothing().when(coordinator).putState(any(Coordinator.State.class));
    doNothing().when(handler).rollbackRecords(any(Snapshot.class));

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot))
        .isInstanceOf(CommitConflictException.class)
        .hasCause(toThrow);

    // Assert
    verify(coordinator).putState(new Coordinator.State(ANY_ID, TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(ANY_ID, TransactionState.COMMITTED));
    verify(handler).rollbackRecords(snapshot);
  }

  @Test
  public void commit_ExceptionThrownInPrepareRecords_ShouldAbortAndRollbackRecords()
      throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    ExecutionException toThrow = mock(ExecutionException.class);
    doThrow(toThrow).when(storage).mutate(anyList());
    doNothing().when(coordinator).putState(any(Coordinator.State.class));
    doNothing().when(handler).rollbackRecords(any(Snapshot.class));

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot))
        .isInstanceOf(CommitException.class)
        .hasCause(toThrow);

    // Assert
    verify(coordinator).putState(new Coordinator.State(ANY_ID, TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(ANY_ID, TransactionState.COMMITTED));
    verify(handler).rollbackRecords(snapshot);
  }

  @Test
  public void
      commit_ExceptionThrownInPrepareRecordsAndFailedInCoordinatorAbortThenAbortedReturnedInGetState_ShouldRollbackRecords()
          throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    ExecutionException toThrow1 = mock(ExecutionException.class);
    CoordinatorException toThrow2 = mock(CoordinatorException.class);
    doThrow(toThrow1).when(storage).mutate(anyList());
    doThrow(toThrow2)
        .when(coordinator)
        .putState(new Coordinator.State(ANY_ID, TransactionState.ABORTED));
    // assume that it will call Coordinator.getState() if Coordinator.putState() fails
    doReturn(Optional.of(new Coordinator.State(ANY_ID, TransactionState.ABORTED)))
        .when(coordinator)
        .getState(ANY_ID);
    doNothing().when(handler).rollbackRecords(any(Snapshot.class));

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot)).isInstanceOf(CommitException.class);

    // Assert
    verify(coordinator).putState(new Coordinator.State(ANY_ID, TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(ANY_ID, TransactionState.COMMITTED));
    verify(coordinator).getState(ANY_ID);
    verify(handler).rollbackRecords(snapshot);
  }

  @Test
  public void
      commit_ExceptionThrownInPrepareRecordsAndFailedInCoordinatorAbortThenNothingReturnedInGetState_ShouldThrowUnknown()
          throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    ExecutionException toThrow1 = mock(ExecutionException.class);
    CoordinatorException toThrow2 = mock(CoordinatorException.class);
    doThrow(toThrow1).when(storage).mutate(anyList());
    doThrow(toThrow2)
        .when(coordinator)
        .putState(new Coordinator.State(ANY_ID, TransactionState.ABORTED));
    // assume that it will call Coordinator.getState() if Coordinator.putState() fails
    doReturn(Optional.empty()).when(coordinator).getState(ANY_ID);

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert
    verify(coordinator).putState(new Coordinator.State(ANY_ID, TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(ANY_ID, TransactionState.COMMITTED));
    verify(coordinator).getState(ANY_ID);
    verify(handler, never()).rollbackRecords(snapshot);
  }

  @Test
  public void
      commit_ExceptionThrownInPrepareRecordsAndFailedInCoordinatorAbortThenExceptionThrownInGetState_ShouldThrowUnknown()
          throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    ExecutionException toThrow1 = mock(ExecutionException.class);
    CoordinatorException toThrow2 = mock(CoordinatorException.class);
    doThrow(toThrow1).when(storage).mutate(anyList());
    doThrow(toThrow2)
        .when(coordinator)
        .putState(new Coordinator.State(ANY_ID, TransactionState.ABORTED));
    // assume that it will call Coordinator.getState() if Coordinator.putState() fails
    doThrow(toThrow2).when(coordinator).getState(ANY_ID);

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert
    verify(coordinator).putState(new Coordinator.State(ANY_ID, TransactionState.ABORTED));
    verify(coordinator, never())
        .putState(new Coordinator.State(ANY_ID, TransactionState.COMMITTED));
    verify(coordinator).getState(ANY_ID);
    verify(handler, never()).rollbackRecords(snapshot);
  }

  @Test
  public void
      commit_ExceptionThrownInCoordinatorCommitAndSucceededInCoordinatorAbort_ShouldRollbackRecords()
          throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    CoordinatorException toThrow = mock(CoordinatorException.class);
    doNothing().when(storage).mutate(anyList());
    doThrow(toThrow)
        .when(coordinator)
        .putState(new Coordinator.State(ANY_ID, TransactionState.COMMITTED));
    doNothing().when(coordinator).putState(new Coordinator.State(ANY_ID, TransactionState.ABORTED));
    doNothing().when(handler).rollbackRecords(any(Snapshot.class));

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot))
        .isInstanceOf(CommitException.class)
        .hasCause(toThrow);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verify(coordinator).putState(new Coordinator.State(ANY_ID, TransactionState.COMMITTED));
    verify(coordinator).putState(new Coordinator.State(ANY_ID, TransactionState.ABORTED));
    verify(handler).rollbackRecords(snapshot);
  }

  @Test
  public void
      commit_ExceptionThrownInCoordinatorCommitAndAbortAndCommittedReturnedInGetState_ShouldCommitRecords()
          throws ExecutionException, CommitException, UnknownTransactionStatusException,
              CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    CoordinatorException toThrow = mock(CoordinatorException.class);
    doNothing().when(storage).mutate(anyList());
    doThrow(toThrow)
        .when(coordinator)
        .putState(new Coordinator.State(ANY_ID, TransactionState.COMMITTED));
    doThrow(toThrow)
        .when(coordinator)
        .putState(new Coordinator.State(ANY_ID, TransactionState.ABORTED));
    // assume that it will call Coordinator.getState() if Coordinator.putState() fails
    doReturn(Optional.of(new Coordinator.State(ANY_ID, TransactionState.COMMITTED)))
        .when(coordinator)
        .getState(ANY_ID);

    // Act
    handler.commit(snapshot);

    // Assert
    verify(storage, times(4)).mutate(anyList());
    verify(coordinator).putState(new Coordinator.State(ANY_ID, TransactionState.COMMITTED));
    verify(coordinator).putState(new Coordinator.State(ANY_ID, TransactionState.ABORTED));
    verify(coordinator).getState(ANY_ID);
    verify(handler, never()).rollbackRecords(snapshot);
  }

  @Test
  public void
      commit_ExceptionThrownInCoordinatorCommitAndAbortAndAbortedReturnedInGetState_ShouldRollbackRecords()
          throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    CoordinatorException toThrow = mock(CoordinatorException.class);
    doNothing().when(storage).mutate(anyList());
    doThrow(toThrow)
        .when(coordinator)
        .putState(new Coordinator.State(ANY_ID, TransactionState.COMMITTED));
    doThrow(toThrow)
        .when(coordinator)
        .putState(new Coordinator.State(ANY_ID, TransactionState.ABORTED));
    // assume that it will call Coordinator.getState() if Coordinator.putState() fails
    doReturn(Optional.of(new Coordinator.State(ANY_ID, TransactionState.ABORTED)))
        .when(coordinator)
        .getState(ANY_ID);
    doNothing().when(handler).rollbackRecords(any(Snapshot.class));

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot)).isInstanceOf(CommitException.class);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verify(coordinator).putState(new Coordinator.State(ANY_ID, TransactionState.COMMITTED));
    verify(coordinator).putState(new Coordinator.State(ANY_ID, TransactionState.ABORTED));
    verify(coordinator).getState(ANY_ID);
    verify(handler).rollbackRecords(snapshot);
  }

  @Test
  public void
      commit_ExceptionThrownInCoordinatorCommitAndAbortAndNothingReturnedInGetState_ShouldThrowUnknown()
          throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    CoordinatorException toThrow = mock(CoordinatorException.class);
    doNothing().when(storage).mutate(anyList());
    doThrow(toThrow)
        .when(coordinator)
        .putState(new Coordinator.State(ANY_ID, TransactionState.COMMITTED));
    doThrow(toThrow)
        .when(coordinator)
        .putState(new Coordinator.State(ANY_ID, TransactionState.ABORTED));
    // assume that it will call Coordinator.getState() if Coordinator.putState() fails
    doReturn(Optional.empty()).when(coordinator).getState(ANY_ID);

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verify(coordinator).putState(new Coordinator.State(ANY_ID, TransactionState.COMMITTED));
    verify(coordinator).putState(new Coordinator.State(ANY_ID, TransactionState.ABORTED));
    verify(coordinator).getState(ANY_ID);
    verify(handler, never()).rollbackRecords(snapshot);
  }

  @Test
  public void
      commit_ExceptionThrownInCoordinatorCommitAndAbortAndExceptionThrownInGetState_ShouldThrowUnknown()
          throws ExecutionException, CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    CoordinatorException toThrow = mock(CoordinatorException.class);
    doNothing().when(storage).mutate(anyList());
    doThrow(toThrow)
        .when(coordinator)
        .putState(new Coordinator.State(ANY_ID, TransactionState.COMMITTED));
    doThrow(toThrow)
        .when(coordinator)
        .putState(new Coordinator.State(ANY_ID, TransactionState.ABORTED));
    // assume that it will call Coordinator.getState() if Coordinator.putState() fails
    doThrow(toThrow).when(coordinator).getState(ANY_ID);

    // Act
    assertThatThrownBy(() -> handler.commit(snapshot))
        .isInstanceOf(UnknownTransactionStatusException.class);

    // Assert
    verify(storage, times(2)).mutate(anyList());
    verify(coordinator).putState(new Coordinator.State(ANY_ID, TransactionState.COMMITTED));
    verify(coordinator).putState(new Coordinator.State(ANY_ID, TransactionState.ABORTED));
    verify(coordinator).getState(ANY_ID);
    verify(handler, never()).rollbackRecords(snapshot);
  }

  @Test
  public void commit_ExceptionThrownInCommitRecords_ShouldBeIgnored()
      throws ExecutionException, CommitException, UnknownTransactionStatusException,
          CoordinatorException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithDifferentPartitionPut();
    ExecutionException toThrow = mock(ExecutionException.class);
    doNothing().doNothing().doThrow(toThrow).when(storage).mutate(anyList());
    doNothing()
        .when(coordinator)
        .putState(new Coordinator.State(ANY_ID, TransactionState.COMMITTED));

    // Act
    handler.commit(snapshot);

    // Assert
    verify(storage, times(4)).mutate(anyList());
    verify(coordinator).putState(new Coordinator.State(ANY_ID, TransactionState.COMMITTED));
    verify(coordinator, never()).putState(new Coordinator.State(ANY_ID, TransactionState.ABORTED));
    verify(handler, never()).rollbackRecords(snapshot);
  }
}
