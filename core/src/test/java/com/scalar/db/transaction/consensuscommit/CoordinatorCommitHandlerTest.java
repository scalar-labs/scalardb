package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.scalar.db.api.Put;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet;
import java.util.Objects;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Direct unit tests for {@link CoordinatorCommitHandler}. */
class CoordinatorCommitHandlerTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_ID = "id";
  private static final int ANY_INT_1 = 100;

  @Mock private Coordinator coordinator;
  @Mock private TransactionTableMetadataManager tableMetadataManager;
  @Mock private ParticipantCommitHandler participantCommitHandler;
  @Mock private ConsensusCommitConfig config;

  private ParallelExecutor parallelExecutor;
  private CoordinatorCommitHandler handler;

  private String anyId() {
    return ANY_ID;
  }

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    parallelExecutor = new ParallelExecutor(config);
    handler = createHandler();
  }

  @AfterEach
  void tearDown() {
    parallelExecutor.close();
  }

  private CoordinatorCommitHandler createHandler() {
    return new CoordinatorCommitHandler(
        coordinator, new WriteSetEncoder(tableMetadataManager), participantCommitHandler);
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

  private Snapshot prepareSnapshotWithWrite() throws CrudException {
    Snapshot snapshot = new Snapshot(anyId(), tableMetadataManager, new ParallelExecutor(config));
    Put put1 = preparePut1();
    snapshot.putIntoWriteSet(new Snapshot.Key(put1), put1);
    return snapshot;
  }

  private TransactionContext createTransactionContext(Snapshot snapshot) {
    return new TransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);
  }

  // Helper: encode the WriteSet that would be persisted for the given snapshot/context.
  private WriteSet expectedSingleGroupWriteSet(Snapshot snapshot) {
    TransactionContext context = createTransactionContext(snapshot);
    return new WriteSetEncoder(tableMetadataManager).encodeSingleGroupWriteSet(context, false);
  }

  // Mockito matcher that compares Coordinator.State by id + writeSet + state (ignores createdAt).
  private static ArgumentMatcher<Coordinator.State> stateMatcher(
      String id, WriteSet writeSet, TransactionState state) {
    return actual ->
        actual != null
            && Objects.equals(actual.getId(), id)
            && actual.getWriteSet().equals(Optional.ofNullable(writeSet))
            && actual.getState() == state;
  }

  // ---------- commitState ----------

  @Test
  void commitState_WhenSuccessful_ShouldPutCommittedState()
      throws CommitConflictException, UnknownTransactionStatusException, CoordinatorException,
          CrudException {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithWrite();
    TransactionContext context = createTransactionContext(snapshot);
    doNothing().when(coordinator).putState(any(Coordinator.State.class));

    // Act
    handler.commitState(context);

    // Assert
    verify(coordinator)
        .putState(
            argThat(
                stateMatcher(
                    anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.COMMITTED)));
  }

  @Test
  void
      commitState_WhenCoordinatorConflictAndAbortedReturnedInGetState_ShouldRollbackRecordsAndThrowConflict()
          throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithWrite();
    TransactionContext context = createTransactionContext(snapshot);
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(
            argThat(
                stateMatcher(
                    anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.COMMITTED)));
    doReturn(
            Optional.of(
                new Coordinator.State(
                    anyId(), TransactionState.ABORTED, System.currentTimeMillis())))
        .when(coordinator)
        .getState(anyId());

    // Act Assert
    assertThatThrownBy(() -> handler.commitState(context))
        .isInstanceOf(CommitConflictException.class);

    verify(participantCommitHandler).rollbackRecords(context);
  }

  @Test
  void commitState_WhenCoordinatorConflictAndCommittedReturnedInGetState_ShouldReturnNormally()
      throws Exception {
    // Two-phase Commit can drive the same commit twice (multiple participants / recovery), so the
    // COMMITTED case is reachable and treated as success.

    // Arrange
    Snapshot snapshot = prepareSnapshotWithWrite();
    TransactionContext context = createTransactionContext(snapshot);
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(
            argThat(
                stateMatcher(
                    anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.COMMITTED)));
    doReturn(
            Optional.of(
                new Coordinator.State(
                    anyId(), TransactionState.COMMITTED, System.currentTimeMillis())))
        .when(coordinator)
        .getState(anyId());

    // Act (must not throw)
    handler.commitState(context);

    // Assert
    verify(participantCommitHandler, never()).rollbackRecords(any());
  }

  @Test
  void
      commitState_WhenCoordinatorConflictAndNoStatePersisted_ShouldRollbackRecordsAndThrowConflict()
          throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithWrite();
    TransactionContext context = createTransactionContext(snapshot);
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(
            argThat(
                stateMatcher(
                    anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.COMMITTED)));
    doReturn(Optional.empty()).when(coordinator).getState(anyId());

    // Act Assert
    assertThatThrownBy(() -> handler.commitState(context))
        .isInstanceOf(CommitConflictException.class);

    verify(participantCommitHandler).rollbackRecords(context);
  }

  @Test
  void commitState_WhenCoordinatorExceptionThrown_ShouldThrowUnknown() throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithWrite();
    TransactionContext context = createTransactionContext(snapshot);
    doThrow(CoordinatorException.class).when(coordinator).putState(any(Coordinator.State.class));

    // Act Assert
    assertThatThrownBy(() -> handler.commitState(context))
        .isInstanceOf(UnknownTransactionStatusException.class);
  }

  // ---------- commitStateWithoutWriteSet ----------

  @Test
  void commitStateWithoutWriteSet_WhenSuccessful_ShouldPutCommittedStateWithoutWriteSet()
      throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithWrite();
    TransactionContext context = createTransactionContext(snapshot);
    doNothing().when(coordinator).putState(any(Coordinator.State.class));

    // Act
    handler.commitStateWithoutWriteSet(context);

    // Assert — writeSet absent (null in matcher).
    verify(coordinator).putState(argThat(stateMatcher(anyId(), null, TransactionState.COMMITTED)));
  }

  // ---------- abortState ----------

  @Test
  void abortState_WhenSuccessful_ShouldPutAbortedStateAndReturnAborted() throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithWrite();
    TransactionContext context = createTransactionContext(snapshot);
    doNothing().when(coordinator).putState(any(Coordinator.State.class));

    // Act
    TransactionState result = handler.abortState(context);

    // Assert
    assertThat(result).isEqualTo(TransactionState.ABORTED);
    verify(coordinator)
        .putState(
            argThat(
                stateMatcher(
                    anyId(), expectedSingleGroupWriteSet(snapshot), TransactionState.ABORTED)));
  }

  @Test
  void abortState_WhenConflictAndCommittedStatePersisted_ShouldReturnCommitted() throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithWrite();
    TransactionContext context = createTransactionContext(snapshot);
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(any(Coordinator.State.class));
    doReturn(
            Optional.of(
                new Coordinator.State(
                    anyId(), TransactionState.COMMITTED, System.currentTimeMillis())))
        .when(coordinator)
        .getState(anyId());

    // Act
    TransactionState result = handler.abortState(context);

    // Assert
    assertThat(result).isEqualTo(TransactionState.COMMITTED);
  }

  @Test
  void abortState_WhenConflictAndNoStatePersisted_ShouldReturnAborted() throws Exception {
    // Unlike forceAbortState (the One-phase Commit I/F abort-by-id path), abortState is used by the
    // self-abort path and all Two-phase Commit I/F aborts, which cannot be racing a real, deletable
    // COMMITTED row. A conflicting-then-absent coordinator state is therefore determinable as
    // ABORTED (the conflict was a lazy-recovery ABORTED later removed by the Coordinator state
    // cleanup process), not an honest UNKNOWN.

    // Arrange
    Snapshot snapshot = prepareSnapshotWithWrite();
    TransactionContext context = createTransactionContext(snapshot);
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(any(Coordinator.State.class));
    doReturn(Optional.empty()).when(coordinator).getState(anyId());

    // Act
    TransactionState result = handler.abortState(context);

    // Assert
    assertThat(result).isEqualTo(TransactionState.ABORTED);
    verify(coordinator).getState(anyId());
  }

  @Test
  void abortState_WhenCoordinatorExceptionThrown_ShouldThrowUnknown() throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithWrite();
    TransactionContext context = createTransactionContext(snapshot);
    doThrow(CoordinatorException.class).when(coordinator).putState(any(Coordinator.State.class));

    // Act Assert
    assertThatThrownBy(() -> handler.abortState(context))
        .isInstanceOf(UnknownTransactionStatusException.class);
  }

  // ---------- abortStateWithoutWriteSet ----------

  @Test
  void abortStateWithoutWriteSet_WhenSuccessful_ShouldPutAbortedStateWithoutWriteSet()
      throws Exception {
    // Arrange
    doNothing().when(coordinator).putState(any(Coordinator.State.class));

    // Act
    TransactionState result = handler.abortStateWithoutWriteSet(anyId());

    // Assert
    assertThat(result).isEqualTo(TransactionState.ABORTED);
    verify(coordinator).putState(argThat(stateMatcher(anyId(), null, TransactionState.ABORTED)));
  }

  @Test
  void abortStateWithoutWriteSet_WhenConflictAndCommittedStatePersisted_ShouldReturnCommitted()
      throws Exception {
    // Arrange
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(any(Coordinator.State.class));
    doReturn(
            Optional.of(
                new Coordinator.State(
                    anyId(), TransactionState.COMMITTED, System.currentTimeMillis())))
        .when(coordinator)
        .getState(anyId());

    // Act
    TransactionState result = handler.abortStateWithoutWriteSet(anyId());

    // Assert
    assertThat(result).isEqualTo(TransactionState.COMMITTED);
    verify(coordinator).getState(anyId());
  }

  @Test
  void abortStateWithoutWriteSet_WhenConflictAndNoStatePersisted_ShouldReturnAborted()
      throws Exception {
    // Like abortState, this path cannot be racing a deletable COMMITTED row, so a
    // conflicting-then-absent coordinator state resolves to ABORTED, not UNKNOWN.

    // Arrange
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(any(Coordinator.State.class));
    doReturn(Optional.empty()).when(coordinator).getState(anyId());

    // Act
    TransactionState result = handler.abortStateWithoutWriteSet(anyId());

    // Assert
    assertThat(result).isEqualTo(TransactionState.ABORTED);
    verify(coordinator).getState(anyId());
  }

  // ---------- forceAbortState ----------

  @Test
  void forceAbortState_ShouldForceAbortAndReturnAborted()
      throws CoordinatorException, UnknownTransactionStatusException {
    // Arrange
    doNothing().when(coordinator).forceAbort(anyId());

    // Act
    TransactionState result = handler.forceAbortState(anyId());

    // Assert
    assertThat(result).isEqualTo(TransactionState.ABORTED);
    verify(coordinator).forceAbort(anyId());
    verify(coordinator, never()).putState(any());
  }

  @Test
  void forceAbortState_WhenConflictAndCommittedStatePersisted_ShouldReturnCommitted()
      throws CoordinatorException, UnknownTransactionStatusException {
    // Arrange
    doThrow(CoordinatorConflictException.class).when(coordinator).forceAbort(anyId());
    doReturn(
            Optional.of(
                new Coordinator.State(
                    anyId(), TransactionState.COMMITTED, System.currentTimeMillis())))
        .when(coordinator)
        .getState(anyId());

    // Act
    TransactionState result = handler.forceAbortState(anyId());

    // Assert
    assertThat(result).isEqualTo(TransactionState.COMMITTED);
    verify(coordinator).getState(anyId());
  }

  @Test
  void forceAbortState_WhenConflictAndNoStatePersisted_ShouldThrowUnknown()
      throws CoordinatorException {
    // Arrange
    doThrow(CoordinatorConflictException.class).when(coordinator).forceAbort(anyId());
    doReturn(Optional.empty()).when(coordinator).getState(anyId());

    // Act Assert
    assertThatThrownBy(() -> handler.forceAbortState(anyId()))
        .isInstanceOf(UnknownTransactionStatusException.class)
        .hasCauseInstanceOf(CoordinatorConflictException.class);
    verify(coordinator).getState(anyId());
  }

  @Test
  void forceAbortState_WhenCoordinatorExceptionThrown_ShouldThrowUnknown()
      throws CoordinatorException {
    // Arrange
    doThrow(CoordinatorException.class).when(coordinator).forceAbort(anyId());

    // Act Assert
    assertThatThrownBy(() -> handler.forceAbortState(anyId()))
        .isInstanceOf(UnknownTransactionStatusException.class);
  }

  // ---------- handleCommitConflict ----------

  @Test
  void handleCommitConflict_WhenAbortedStatePersisted_ShouldRollbackRecordsAndThrowConflict()
      throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithWrite();
    TransactionContext context = createTransactionContext(snapshot);
    doReturn(
            Optional.of(
                new Coordinator.State(
                    anyId(), TransactionState.ABORTED, System.currentTimeMillis())))
        .when(coordinator)
        .getState(anyId());
    Exception cause = new RuntimeException("conflict");

    // Act Assert
    assertThatThrownBy(() -> handler.handleCommitConflict(context, cause))
        .isInstanceOf(CommitConflictException.class)
        .hasCause(cause);
    verify(participantCommitHandler).rollbackRecords(context);
  }

  @Test
  void handleCommitConflict_WhenCommittedStatePersisted_ShouldReturnNormally() throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithWrite();
    TransactionContext context = createTransactionContext(snapshot);
    doReturn(
            Optional.of(
                new Coordinator.State(
                    anyId(), TransactionState.COMMITTED, System.currentTimeMillis())))
        .when(coordinator)
        .getState(anyId());

    // Act (must not throw)
    handler.handleCommitConflict(context, new RuntimeException("conflict"));

    // Assert
    verify(participantCommitHandler, never()).rollbackRecords(any());
  }

  @Test
  void handleCommitConflict_WhenNoStatePersisted_ShouldRollbackRecordsAndThrowConflict()
      throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithWrite();
    TransactionContext context = createTransactionContext(snapshot);
    doReturn(Optional.empty()).when(coordinator).getState(anyId());
    Exception cause = new RuntimeException("conflict");

    // Act Assert
    assertThatThrownBy(() -> handler.handleCommitConflict(context, cause))
        .isInstanceOf(CommitConflictException.class)
        .hasCause(cause);
    verify(participantCommitHandler).rollbackRecords(context);
  }

  @Test
  void handleCommitConflict_WhenGetStateThrowsCoordinatorException_ShouldThrowUnknown()
      throws Exception {
    // Arrange
    Snapshot snapshot = prepareSnapshotWithWrite();
    TransactionContext context = createTransactionContext(snapshot);
    doThrow(CoordinatorException.class).when(coordinator).getState(anyId());

    // Act Assert
    assertThatThrownBy(() -> handler.handleCommitConflict(context, new RuntimeException()))
        .isInstanceOf(UnknownTransactionStatusException.class);
  }
}
