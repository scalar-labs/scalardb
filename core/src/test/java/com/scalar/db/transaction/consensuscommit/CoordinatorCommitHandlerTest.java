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

import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet;
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
    // Two-phase Commit can drive the same commit twice (multiple participants / recovery), so the
    // COMMITTED case is reachable and treated as success — the persisted row's committedAt is
    // returned, not the value we tried to write.

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
    // Unlike forceAbortState (the One-phase Commit I/F abort-by-id path), abortState is used by the
    // self-abort path and all Two-phase Commit I/F aborts, which cannot be racing a real, deletable
    // COMMITTED row. A conflicting-then-absent coordinator state is therefore determinable as
    // ABORTED (the conflict was a lazy-recovery ABORTED later removed by the Coordinator state
    // cleanup process), not an honest UNKNOWN.

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
}
