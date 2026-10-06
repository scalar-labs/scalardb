package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.TransactionState;
import com.scalar.db.common.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Direct unit tests for {@link CoordinatorCommitHandler}. These tests pass ids and pre-encoded
 * write sets directly; encoding is the orchestrator's job, covered by {@link CommitHandlerTest}.
 */
class CoordinatorCommitHandlerTest {
  private static final String ANY_ID = "id";

  @Mock private CoordinatorStateAccessor coordinator;

  private CoordinatorCommitHandler handler;

  private String anyId() {
    return ANY_ID;
  }

  // A non-empty pre-encoded write set; its contents are irrelevant here (the handler just persists
  // it verbatim), so any WriteSet that round-trips through the State matcher works.
  private static WriteSet anyWriteSet() {
    return WriteSet.newBuilder().setSchemaVersion(1).build();
  }

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    handler = new CoordinatorCommitHandler(coordinator);
  }

  // Mockito matcher that compares CoordinatorStateAccessor.State by id + writeSet + state (ignores
  // createdAt).
  private static ArgumentMatcher<CoordinatorStateAccessor.State> stateMatcher(
      String id, WriteSet writeSet, TransactionState state) {
    return actual ->
        actual != null
            && Objects.equals(actual.getId(), id)
            && actual.getWriteSet().equals(Optional.ofNullable(writeSet))
            && actual.getState() == state;
  }

  // ---------- commitState ----------

  @Test
  void commitState_WhenSuccessful_ShouldPutCommittedStateWithGivenWriteSet() throws Exception {
    // Arrange
    WriteSet writeSet = anyWriteSet();
    doNothing().when(coordinator).putState(any(CoordinatorStateAccessor.State.class));

    // Act
    handler.commitState(anyId(), writeSet);

    // Assert
    verify(coordinator)
        .putState(argThat(stateMatcher(anyId(), writeSet, TransactionState.COMMITTED)));
  }

  @Test
  void commitState_WhenWriteSetNull_ShouldPutCommittedStateWithoutWriteSet() throws Exception {
    // Arrange
    doNothing().when(coordinator).putState(any(CoordinatorStateAccessor.State.class));

    // Act
    handler.commitState(anyId(), null);

    // Assert — writeSet absent (null in matcher).
    verify(coordinator).putState(argThat(stateMatcher(anyId(), null, TransactionState.COMMITTED)));
  }

  @Test
  void commitState_ShouldStampGeneratedCommittedAtAndReturnIt() throws Exception {
    // Arrange
    doNothing().when(coordinator).putState(any(CoordinatorStateAccessor.State.class));

    // Act
    long committedAt = handler.commitState(anyId(), anyWriteSet());

    // Assert — the handler generates the committedAt, stamps it on the COMMITTED row, and returns
    // that same value.
    ArgumentCaptor<CoordinatorStateAccessor.State> captor =
        ArgumentCaptor.forClass(CoordinatorStateAccessor.State.class);
    verify(coordinator).putState(captor.capture());
    assertThat(captor.getValue().getCreatedAt()).isEqualTo(committedAt);
  }

  @Test
  void commitState_WhenCoordinatorConflictAndAbortedReturnedInGetState_ShouldThrowConflict()
      throws Exception {
    // Record rollback is the orchestrator's job (see CommitHandlerTest); this only checks the
    // conflict surfaces.
    // Arrange
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(any(CoordinatorStateAccessor.State.class));
    doReturn(
            Optional.of(
                new CoordinatorStateAccessor.State(
                    anyId(), TransactionState.ABORTED, System.currentTimeMillis())))
        .when(coordinator)
        .getState(anyId());

    // Act Assert
    assertThatThrownBy(() -> handler.commitState(anyId(), anyWriteSet()))
        .isInstanceOf(CommitConflictException.class);
  }

  @Test
  void commitState_WhenCoordinatorConflictAndCommittedReturnedInGetState_ShouldReturnPersisted()
      throws Exception {
    // A retried putIfNotExists can lose the race to this commit's own earlier attempt that was
    // applied although reported as failed, so the COMMITTED case is reachable and treated as
    // success — the persisted row's committedAt is returned.

    // Arrange
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(any(CoordinatorStateAccessor.State.class));
    doReturn(
            Optional.of(
                new CoordinatorStateAccessor.State(anyId(), TransactionState.COMMITTED, 999L)))
        .when(coordinator)
        .getState(anyId());

    // Act (must not throw)
    long committedAt = handler.commitState(anyId(), anyWriteSet());

    // Assert
    assertThat(committedAt).isEqualTo(999L);
  }

  @Test
  void commitState_WhenCoordinatorConflictAndNoStatePersisted_ShouldThrowConflict()
      throws Exception {
    // Record rollback is the orchestrator's job (see CommitHandlerTest); this only checks the
    // conflict surfaces.
    // Arrange
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(any(CoordinatorStateAccessor.State.class));
    doReturn(Optional.empty()).when(coordinator).getState(anyId());

    // Act Assert
    assertThatThrownBy(() -> handler.commitState(anyId(), anyWriteSet()))
        .isInstanceOf(CommitConflictException.class);
  }

  @Test
  void commitState_WhenCoordinatorExceptionThrown_ShouldThrowUnknown() throws Exception {
    // Arrange
    doThrow(CoordinatorException.class)
        .when(coordinator)
        .putState(any(CoordinatorStateAccessor.State.class));

    // Act Assert
    assertThatThrownBy(() -> handler.commitState(anyId(), anyWriteSet()))
        .isInstanceOf(UnknownTransactionStatusException.class);
  }

  // ---------- abortState ----------

  @Test
  void abortState_WhenSuccessful_ShouldPutAbortedStateAndReturnAborted() throws Exception {
    // Arrange
    WriteSet writeSet = anyWriteSet();
    doNothing().when(coordinator).putState(any(CoordinatorStateAccessor.State.class));

    // Act
    TransactionState result = handler.abortState(anyId(), writeSet);

    // Assert
    assertThat(result).isEqualTo(TransactionState.ABORTED);
    verify(coordinator)
        .putState(argThat(stateMatcher(anyId(), writeSet, TransactionState.ABORTED)));
  }

  @Test
  void abortState_WhenWriteSetNull_ShouldPutAbortedStateWithoutWriteSet() throws Exception {
    // Arrange
    doNothing().when(coordinator).putState(any(CoordinatorStateAccessor.State.class));

    // Act
    TransactionState result = handler.abortState(anyId(), null);

    // Assert
    assertThat(result).isEqualTo(TransactionState.ABORTED);
    verify(coordinator).putState(argThat(stateMatcher(anyId(), null, TransactionState.ABORTED)));
  }

  @Test
  void abortState_WhenConflictAndCommittedStatePersisted_ShouldReturnCommitted() throws Exception {
    // Arrange
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(any(CoordinatorStateAccessor.State.class));
    doReturn(
            Optional.of(
                new CoordinatorStateAccessor.State(
                    anyId(), TransactionState.COMMITTED, System.currentTimeMillis())))
        .when(coordinator)
        .getState(anyId());

    // Act
    TransactionState result = handler.abortState(anyId(), anyWriteSet());

    // Assert
    assertThat(result).isEqualTo(TransactionState.COMMITTED);
  }

  @Test
  void abortState_WhenConflictAndNoStatePersisted_ShouldReturnAborted() throws Exception {
    // Unlike forceAbortState (the abort-by-id path), abortState is used only on the self-abort path
    // -- a commit orchestrator aborting its own transaction after a prepare or validate failure,
    // before commitState -- and so cannot be racing a real, deletable COMMITTED row. A
    // conflicting-then-absent coordinator state is therefore determinable as ABORTED (the conflict
    // was a lazy-recovery ABORTED later removed by the Coordinator state cleanup process), not an
    // honest UNKNOWN.

    // Arrange
    doThrow(CoordinatorConflictException.class)
        .when(coordinator)
        .putState(any(CoordinatorStateAccessor.State.class));
    doReturn(Optional.empty()).when(coordinator).getState(anyId());

    // Act
    TransactionState result = handler.abortState(anyId(), null);

    // Assert
    assertThat(result).isEqualTo(TransactionState.ABORTED);
    verify(coordinator).getState(anyId());
  }

  @Test
  void abortState_WhenCoordinatorExceptionThrown_ShouldThrowUnknown() throws Exception {
    // Arrange
    doThrow(CoordinatorException.class)
        .when(coordinator)
        .putState(any(CoordinatorStateAccessor.State.class));

    // Act Assert
    assertThatThrownBy(() -> handler.abortState(anyId(), null))
        .isInstanceOf(UnknownTransactionStatusException.class);
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
                new CoordinatorStateAccessor.State(
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
  void handleCommitConflict_WhenAbortedStatePersisted_ShouldThrowConflict() throws Exception {
    // Record rollback is the orchestrator's job (see CommitHandlerTest); this only checks the
    // conflict surfaces.
    // Arrange
    doReturn(
            Optional.of(
                new CoordinatorStateAccessor.State(
                    anyId(), TransactionState.ABORTED, System.currentTimeMillis())))
        .when(coordinator)
        .getState(anyId());
    Exception cause = new RuntimeException("conflict");

    // Act Assert
    assertThatThrownBy(() -> handler.handleCommitConflict(anyId(), cause))
        .isInstanceOf(CommitConflictException.class)
        .hasCause(cause);
  }

  @Test
  void handleCommitConflict_WhenCommittedStatePersisted_ShouldReturnPersistedCommittedAt()
      throws Exception {
    // Arrange
    doReturn(
            Optional.of(
                new CoordinatorStateAccessor.State(anyId(), TransactionState.COMMITTED, 999L)))
        .when(coordinator)
        .getState(anyId());

    // Act
    long committedAt = handler.handleCommitConflict(anyId(), new RuntimeException("conflict"));

    // Assert
    assertThat(committedAt).isEqualTo(999L);
  }

  @Test
  void handleCommitConflict_WhenNoStatePersisted_ShouldThrowConflict() throws Exception {
    // Record rollback is the orchestrator's job (see CommitHandlerTest); this only checks the
    // conflict surfaces.
    // Arrange
    doReturn(Optional.empty()).when(coordinator).getState(anyId());
    Exception cause = new RuntimeException("conflict");

    // Act Assert
    assertThatThrownBy(() -> handler.handleCommitConflict(anyId(), cause))
        .isInstanceOf(CommitConflictException.class)
        .hasCause(cause);
  }

  @Test
  void handleCommitConflict_WhenGetStateThrowsCoordinatorException_ShouldThrowUnknown()
      throws Exception {
    // Arrange
    doThrow(CoordinatorException.class).when(coordinator).getState(anyId());

    // Act Assert
    assertThatThrownBy(() -> handler.handleCommitConflict(anyId(), new RuntimeException()))
        .isInstanceOf(UnknownTransactionStatusException.class);
  }

  @Test
  void handleCommitConflict_WhenSentOnceAndAbortedStatePersisted_ShouldThrowConflict()
      throws Exception {
    // Arrange
    doReturn(
            Optional.of(
                new CoordinatorStateAccessor.State(
                    anyId(), TransactionState.ABORTED, System.currentTimeMillis())))
        .when(coordinator)
        .getState(anyId());
    CoordinatorConflictException cause = conflictAfterAttempts(1);

    // Act Assert
    assertThatThrownBy(() -> handler.handleCommitConflict(anyId(), cause))
        .isInstanceOf(CommitConflictException.class)
        .hasCause(cause);
  }

  @Test
  void handleCommitConflict_WhenSentOnceAndNoStatePersisted_ShouldThrowConflict() throws Exception {
    // Arrange
    doReturn(Optional.empty()).when(coordinator).getState(anyId());
    CoordinatorConflictException cause = conflictAfterAttempts(1);

    // Act Assert
    assertThatThrownBy(() -> handler.handleCommitConflict(anyId(), cause))
        .isInstanceOf(CommitConflictException.class)
        .hasCause(cause);
  }

  @Test
  void handleCommitConflict_WhenSentMoreThanOnceAndAbortedStatePersisted_ShouldThrowUnknown()
      throws Exception {
    // The ABORTED row may have been written for this transaction after its own COMMITTED row was
    // removed by finishTransaction, so the records must not be rolled back.
    // Arrange
    doReturn(
            Optional.of(
                new CoordinatorStateAccessor.State(
                    anyId(), TransactionState.ABORTED, System.currentTimeMillis())))
        .when(coordinator)
        .getState(anyId());
    CoordinatorConflictException cause = conflictAfterAttempts(2);

    // Act Assert
    assertThatThrownBy(() -> handler.handleCommitConflict(anyId(), cause))
        .isInstanceOf(UnknownTransactionStatusException.class)
        .hasMessageContaining(
            CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHEN_COMMITTING_STATE_AFTER_RETRY
                .buildCode())
        .hasCause(cause);
  }

  @Test
  void handleCommitConflict_WhenSentMoreThanOnceAndNoStatePersisted_ShouldThrowUnknown()
      throws Exception {
    // The conflicting row may have been this transaction's own COMMITTED row, removed by
    // finishTransaction since, so the records must not be rolled back.
    // Arrange
    doReturn(Optional.empty()).when(coordinator).getState(anyId());
    CoordinatorConflictException cause = conflictAfterAttempts(2);

    // Act Assert
    assertThatThrownBy(() -> handler.handleCommitConflict(anyId(), cause))
        .isInstanceOf(UnknownTransactionStatusException.class)
        .hasMessageContaining(
            CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHEN_COMMITTING_STATE_AFTER_RETRY
                .buildCode())
        .hasCause(cause);
  }

  @Test
  void
      handleCommitConflict_WhenSentMoreThanOnceAndCommittedStatePersisted_ShouldReturnPersistedCommittedAt()
          throws Exception {
    // Arrange
    doReturn(
            Optional.of(
                new CoordinatorStateAccessor.State(anyId(), TransactionState.COMMITTED, 999L)))
        .when(coordinator)
        .getState(anyId());

    // Act
    long committedAt = handler.handleCommitConflict(anyId(), conflictAfterAttempts(2));

    // Assert
    assertThat(committedAt).isEqualTo(999L);
  }

  @Test
  void
      handleCommitConflict_WhenSentMoreThanOnceAndGetStateThrowsCoordinatorException_ShouldThrowUnknownForUnreadableState()
          throws Exception {
    // Arrange
    doThrow(CoordinatorException.class).when(coordinator).getState(anyId());

    // Act Assert
    assertThatThrownBy(() -> handler.handleCommitConflict(anyId(), conflictAfterAttempts(2)))
        .isInstanceOf(UnknownTransactionStatusException.class)
        .hasMessageContaining(CoreError.CONSENSUS_COMMIT_CANNOT_GET_COORDINATOR_STATUS.buildCode());
  }

  @Test
  void
      commitState_WhenCoordinatorConflictAfterMoreThanOneAttemptAndNoStatePersisted_ShouldThrowUnknown()
          throws Exception {
    // Arrange
    doThrow(conflictAfterAttempts(2))
        .when(coordinator)
        .putState(any(CoordinatorStateAccessor.State.class));
    doReturn(Optional.empty()).when(coordinator).getState(anyId());

    // Act Assert
    assertThatThrownBy(() -> handler.commitState(anyId(), anyWriteSet()))
        .isInstanceOf(UnknownTransactionStatusException.class);
  }

  // ---------- commitState with a real CoordinatorStateAccessor ----------
  // These pin the hand-off between the accessor, which records how many times the putState was
  // sent, and the handler, which decides on it.

  @Test
  void
      commitState_WithRealAccessor_WhenPutFailsThenConflictsAndNoStatePersisted_ShouldThrowUnknown()
          throws Exception {
    // Arrange
    DistributedStorage storage = mock(DistributedStorage.class);
    CoordinatorCommitHandler handlerWithRealAccessor =
        new CoordinatorCommitHandler(
            new CoordinatorStateAccessor(storage, mock(ConsensusCommitConfig.class)));
    ExecutionException failure = new ExecutionException("error");
    doThrow(failure)
        .doThrow(new NoMutationException("error", Collections.emptyList()))
        .when(storage)
        .put(any(Put.class));
    doReturn(Optional.empty()).when(storage).get(any(Get.class));

    // Act
    Throwable thrown =
        catchThrowable(() -> handlerWithRealAccessor.commitState(anyId(), anyWriteSet()));

    // Assert
    assertThat(thrown)
        .isInstanceOf(UnknownTransactionStatusException.class)
        .hasMessageContaining(
            CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHEN_COMMITTING_STATE_AFTER_RETRY
                .buildCode());
    assertThat(thrown.getCause()).isInstanceOf(CoordinatorConflictException.class);
    assertThat(thrown.getCause().getSuppressed()).containsExactly(failure);
  }

  @Test
  void
      commitState_WithRealAccessor_WhenConflictsInFirstAttemptAndNoStatePersisted_ShouldThrowConflict()
          throws Exception {
    // Arrange
    DistributedStorage storage = mock(DistributedStorage.class);
    CoordinatorCommitHandler handlerWithRealAccessor =
        new CoordinatorCommitHandler(
            new CoordinatorStateAccessor(storage, mock(ConsensusCommitConfig.class)));
    doThrow(new NoMutationException("error", Collections.emptyList()))
        .when(storage)
        .put(any(Put.class));
    doReturn(Optional.empty()).when(storage).get(any(Get.class));

    // Act Assert
    assertThatThrownBy(() -> handlerWithRealAccessor.commitState(anyId(), anyWriteSet()))
        .isInstanceOf(CommitConflictException.class);
  }

  private static CoordinatorConflictException conflictAfterAttempts(int numAttempts) {
    return new CoordinatorConflictException(
        "conflict", numAttempts, new RuntimeException("no mutation"));
  }
}
