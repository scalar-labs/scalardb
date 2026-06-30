package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.exception.transaction.ValidationException;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link CommitHandler} as an orchestrator: each test pins which {@link
 * ParticipantCommitHandler} and {@link CoordinatorCommitHandler} methods are called (and which are
 * skipped) for each branch of {@code commit()}, plus a delegation test per pass-through method.
 *
 * <p>The participant and coordinator handlers are mocked (not spied); the participant / coordinator
 * behavior tests live in {@link ParticipantCommitHandlerTest} and {@link
 * CoordinatorCommitHandlerTest}. {@link Snapshot} is mocked too -- the orchestrator only reads
 * {@code hasWritesOrDeletes()} from it for branch control.
 */
public class CommitHandlerTest {
  private static final String ANY_ID = "id";

  @Mock protected ParticipantCommitHandler participantCommitHandler;
  @Mock protected CoordinatorCommitHandler coordinatorCommitHandler;
  @Mock protected BeforePreparationHook beforePreparationHook;
  @Mock protected Future<Void> beforePreparationHookFuture;

  protected CommitHandler handler;

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
        coordinatorWriteOmissionOnReadOnlyEnabled,
        coordinatorCommitHandler,
        participantCommitHandler);
  }

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    handler = spy(createCommitHandler(true));
    extraInitialize();
  }

  @AfterEach
  void tearDown() {
    extraCleanup();
  }

  // Builds a mock Snapshot that reports having writes/deletes. The orchestrator only reads
  // hasWritesOrDeletes() from the snapshot; participant / coordinator handlers are mocked, so the
  // snapshot's contents are otherwise irrelevant.
  protected Snapshot snapshotWithWrites() {
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.hasWritesOrDeletes()).thenReturn(true);
    return snapshot;
  }

  // Builds a mock Snapshot that reports no writes/deletes.
  protected Snapshot snapshotWithoutWrites() {
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.hasWritesOrDeletes()).thenReturn(false);
    return snapshot;
  }

  private void setBeforePreparationHookIfNeeded(boolean withBeforePreparationHook) {
    if (withBeforePreparationHook) {
      doReturn(beforePreparationHookFuture).when(beforePreparationHook).handle(any());
      handler.setBeforePreparationHook(beforePreparationHook);
    }
  }

  private void verifyBeforePreparationHook(
      boolean withBeforePreparationHook, TransactionContext context) {
    if (withBeforePreparationHook) {
      verify(beforePreparationHook).handle(eq(context));
    } else {
      verify(beforePreparationHook, never()).handle(any());
    }
  }

  // =========================================================================
  // commit() — happy path
  // =========================================================================

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void commit_WithWrites_ShouldDelegatePrepareValidateCommitStateCommitRecordsInOrder(
      boolean withBeforePreparationHook) throws Exception {
    // Arrange
    Snapshot snapshot = snapshotWithWrites();
    setBeforePreparationHookIfNeeded(withBeforePreparationHook);
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.commit(context);

    // Assert
    InOrder inOrder = inOrder(participantCommitHandler, coordinatorCommitHandler);
    inOrder.verify(participantCommitHandler).prepareRecords(context);
    inOrder.verify(participantCommitHandler).validateRecords(context);
    inOrder.verify(coordinatorCommitHandler).commitState(context);
    inOrder.verify(participantCommitHandler).commitRecords(context);
    verify(participantCommitHandler, never()).rollbackRecords(any());
    verify(coordinatorCommitHandler, never()).abortState(any());
    verifyBeforePreparationHook(withBeforePreparationHook, context);
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  // =========================================================================
  // commit() — read-only / no-writes branches
  // =========================================================================

  @Test
  public void commit_InReadOnlyMode_ShouldOnlyDelegateValidateRecords() throws Exception {
    // Read-only path: coordinator-write omission is enabled by default, so commitState is skipped.
    // Arrange
    Snapshot snapshot = snapshotWithoutWrites();
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, true, false);

    // Act
    handler.commit(context);

    // Assert
    verify(participantCommitHandler).validateRecords(context);
    verify(participantCommitHandler, never()).prepareRecords(any());
    verify(participantCommitHandler, never()).commitRecords(any());
    verify(coordinatorCommitHandler, never()).commitState(any());
    verify(coordinatorCommitHandler, never()).abortState(any());
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  @Test
  public void commit_NoWritesNonReadOnly_OmissionEnabled_ShouldOnlyDelegateValidateRecords()
      throws Exception {
    // Non-read-only tx with empty write set + omission enabled: coordinator state row omitted.
    // Arrange
    Snapshot snapshot = snapshotWithoutWrites();
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.commit(context);

    // Assert
    verify(participantCommitHandler).validateRecords(context);
    verify(participantCommitHandler, never()).prepareRecords(any());
    verify(participantCommitHandler, never()).commitRecords(any());
    verify(coordinatorCommitHandler, never()).commitState(any());
    verify(coordinatorCommitHandler, never()).abortState(any());
  }

  @Test
  public void commit_NoWritesNonReadOnly_OmissionDisabled_ShouldDelegateValidateAndCommitState()
      throws Exception {
    // Non-read-only tx with empty write set + omission disabled: commitState still runs.
    // Arrange
    handler = spy(createCommitHandler(/* coordinatorWriteOmissionOnReadOnlyEnabled= */ false));
    Snapshot snapshot = snapshotWithoutWrites();
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.commit(context);

    // Assert
    verify(participantCommitHandler).validateRecords(context);
    verify(coordinatorCommitHandler).commitState(context);
    verify(participantCommitHandler, never()).prepareRecords(any());
    verify(participantCommitHandler, never()).commitRecords(any());
    verify(coordinatorCommitHandler, never()).abortState(any());
  }

  // =========================================================================
  // commit() — prepareRecords failure
  // =========================================================================

  @Test
  public void commit_PrepareThrowsConflict_ShouldDelegateAbortAndRollbackAndThrowConflict()
      throws Exception {
    // Arrange
    Snapshot snapshot = snapshotWithWrites();
    doThrow(new PreparationConflictException("conflict", anyId()))
        .when(participantCommitHandler)
        .prepareRecords(any());
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitConflictException.class);

    InOrder inOrder = inOrder(participantCommitHandler, coordinatorCommitHandler);
    inOrder.verify(participantCommitHandler).prepareRecords(context);
    inOrder.verify(coordinatorCommitHandler).abortState(context);
    inOrder.verify(participantCommitHandler).rollbackRecords(context);
    verify(coordinatorCommitHandler, never()).commitState(any());
    verify(participantCommitHandler, never()).commitRecords(any());
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void commit_PrepareThrowsGenericPreparationException_ShouldDelegateAbortAndRollback()
      throws Exception {
    // Arrange
    Snapshot snapshot = snapshotWithWrites();
    doThrow(new PreparationException("prep failed", anyId()))
        .when(participantCommitHandler)
        .prepareRecords(any());
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.commit(context))
        .isInstanceOf(CommitException.class)
        .isNotInstanceOf(CommitConflictException.class);

    verify(participantCommitHandler).prepareRecords(context);
    verify(coordinatorCommitHandler).abortState(context);
    verify(participantCommitHandler).rollbackRecords(context);
    verify(handler).onFailureBeforeCommit(context);
  }

  // =========================================================================
  // commit() — validateRecords failure
  // =========================================================================

  @Test
  public void commit_ValidateThrowsConflict_WithWrites_ShouldDelegateAbortAndRollback()
      throws Exception {
    // Arrange
    Snapshot snapshot = snapshotWithWrites();
    doThrow(new ValidationConflictException("conflict", anyId()))
        .when(participantCommitHandler)
        .validateRecords(any());
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitConflictException.class);

    verify(participantCommitHandler).validateRecords(context);
    verify(coordinatorCommitHandler).abortState(context);
    verify(participantCommitHandler).rollbackRecords(context);
    verify(coordinatorCommitHandler, never()).commitState(any());
    verify(participantCommitHandler, never()).commitRecords(any());
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void commit_ValidateThrowsGenericException_WithWrites_ShouldDelegateAbortAndRollback()
      throws Exception {
    // Arrange
    Snapshot snapshot = snapshotWithWrites();
    doThrow(new ValidationException("validate failed", anyId()))
        .when(participantCommitHandler)
        .validateRecords(any());
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.commit(context))
        .isInstanceOf(CommitException.class)
        .isNotInstanceOf(CommitConflictException.class);

    verify(participantCommitHandler).validateRecords(context);
    verify(coordinatorCommitHandler).abortState(context);
    verify(participantCommitHandler).rollbackRecords(context);
  }

  @Test
  public void commit_CommitStateThrowsConflict_WithWrites_ShouldRollbackRecordsAndThrowConflict()
      throws Exception {
    // The Coordinator-side handler only reports the commit-state putState conflict; the
    // orchestrator
    // owns the records, so it rolls them back here before surfacing the conflict.

    // Arrange
    Snapshot snapshot = snapshotWithWrites();
    doThrow(new CommitConflictException("conflict", anyId()))
        .when(coordinatorCommitHandler)
        .commitState(any());
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitConflictException.class);

    verify(coordinatorCommitHandler).commitState(context);
    verify(participantCommitHandler).rollbackRecords(context);
    verify(participantCommitHandler, never()).commitRecords(any());
  }

  @Test
  public void commit_ValidateThrows_NoWrites_OmissionEnabled_ShouldNotDelegateAbortNorRollback()
      throws Exception {
    // abortStateAndRollbackRecordsIfNeeded skips both: !hasWrites + omission.
    // Arrange
    Snapshot snapshot = snapshotWithoutWrites();
    doThrow(new ValidationConflictException("conflict", anyId()))
        .when(participantCommitHandler)
        .validateRecords(any());
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitConflictException.class);

    verify(coordinatorCommitHandler, never()).abortState(any());
    verify(participantCommitHandler, never()).rollbackRecords(any());
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void commit_ValidateThrows_NoWrites_OmissionDisabled_ShouldDelegateAbortStateOnly()
      throws Exception {
    // abortStateAndRollbackRecordsIfNeeded: !hasWrites + !omission → abortState yes, rollback no.
    // Arrange
    handler = spy(createCommitHandler(/* coordinatorWriteOmissionOnReadOnlyEnabled= */ false));
    Snapshot snapshot = snapshotWithoutWrites();
    doThrow(new ValidationConflictException("conflict", anyId()))
        .when(participantCommitHandler)
        .validateRecords(any());
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SERIALIZABLE, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitConflictException.class);

    verify(coordinatorCommitHandler).abortState(context);
    verify(participantCommitHandler, never()).rollbackRecords(any());
  }

  // =========================================================================
  // commit() — commitState failure
  // =========================================================================

  @Test
  public void commit_CommitStateThrowsUnknown_ShouldNotDelegateRollback() throws Exception {
    // The orchestrator does not roll back when commitState throws -- conflict handling lives
    // inside CoordinatorCommitHandler (which may itself roll back via the cross-handler call).
    // Arrange
    Snapshot snapshot = snapshotWithWrites();
    doThrow(new UnknownTransactionStatusException("unknown", anyId()))
        .when(coordinatorCommitHandler)
        .commitState(any());
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.commit(context))
        .isInstanceOf(UnknownTransactionStatusException.class);

    verify(participantCommitHandler).prepareRecords(context);
    verify(participantCommitHandler).validateRecords(context);
    verify(coordinatorCommitHandler).commitState(context);
    verify(participantCommitHandler, never()).commitRecords(any());
    verify(participantCommitHandler, never()).rollbackRecords(any());
    verify(handler, never()).onFailureBeforeCommit(any());
  }

  // =========================================================================
  // commit() — beforePreparationHook
  // =========================================================================

  @Test
  public void commit_BeforePreparationHookFails_WithWrites_ShouldDelegateAbortAndRollback()
      throws Exception {
    // Hook throws synchronously → orchestrator runs the abort+rollback cleanup for write txs.
    // Arrange
    Snapshot snapshot = snapshotWithWrites();
    doThrow(new RuntimeException("Something is wrong")).when(beforePreparationHook).handle(any());
    handler.setBeforePreparationHook(beforePreparationHook);
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    verify(coordinatorCommitHandler).abortState(context);
    verify(participantCommitHandler).rollbackRecords(context);
    verify(participantCommitHandler, never()).prepareRecords(any());
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void commit_BeforePreparationHookFutureFails_WithWrites_ShouldDelegateAbortAndRollback()
      throws Exception {
    // Hook returns a future that fails → orchestrator runs the abort+rollback cleanup after
    // prepare/validate already succeeded.
    // Arrange
    Snapshot snapshot = snapshotWithWrites();
    doThrow(new RuntimeException("Something is wrong")).when(beforePreparationHookFuture).get();
    setBeforePreparationHookIfNeeded(true);
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    verify(participantCommitHandler).prepareRecords(context);
    verify(coordinatorCommitHandler).abortState(context);
    verify(participantCommitHandler).rollbackRecords(context);
    verify(coordinatorCommitHandler, never()).commitState(any());
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void
      commit_BeforePreparationHookFails_ReadOnly_OmissionEnabled_ShouldNotDelegateAbortNorRollback()
          throws Exception {
    // Read-only + omission enabled: no coordinator row written even when the hook fails.
    // Arrange
    Snapshot snapshot = snapshotWithoutWrites();
    doThrow(new RuntimeException("Something is wrong")).when(beforePreparationHook).handle(any());
    handler.setBeforePreparationHook(beforePreparationHook);
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, true, false);

    // Act Assert
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    verify(coordinatorCommitHandler, never()).abortState(any());
    verify(participantCommitHandler, never()).rollbackRecords(any());
    verify(handler).onFailureBeforeCommit(context);
  }

  @Test
  public void
      commit_BeforePreparationHookFails_ReadOnly_OmissionDisabled_ShouldDelegateAbortStateOnly()
          throws Exception {
    // Read-only + omission disabled: ABORTED state still written even though there are no writes.
    // Arrange
    handler = spy(createCommitHandler(/* coordinatorWriteOmissionOnReadOnlyEnabled= */ false));
    Snapshot snapshot = snapshotWithoutWrites();
    doThrow(new RuntimeException("Something is wrong")).when(beforePreparationHook).handle(any());
    handler.setBeforePreparationHook(beforePreparationHook);
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, true, false);

    // Act Assert
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    verify(coordinatorCommitHandler).abortState(context);
    verify(participantCommitHandler, never()).rollbackRecords(any());
  }

  @Test
  public void
      commit_BeforePreparationHookFails_NonReadOnlyNoWrites_OmissionEnabled_ShouldNotDelegateAbortNorRollback()
          throws Exception {
    // Non-read-only tx without writes/deletes + omission enabled: no coordinator row written.
    // Arrange
    Snapshot snapshot = snapshotWithoutWrites();
    doThrow(new RuntimeException("Something is wrong")).when(beforePreparationHook).handle(any());
    handler.setBeforePreparationHook(beforePreparationHook);
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    verify(coordinatorCommitHandler, never()).abortState(any());
    verify(participantCommitHandler, never()).rollbackRecords(any());
  }

  @Test
  public void commit_BeforePreparationHookGiven_ShouldWaitFutureBeforeCommitState()
      throws Exception {
    // The orchestrator must wait on the hook future before invoking commitState.
    // Arrange
    Snapshot snapshot = snapshotWithWrites();
    doReturn(beforePreparationHookFuture).when(beforePreparationHook).handle(any());
    handler.setBeforePreparationHook(beforePreparationHook);
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.commit(context);

    // Assert: the orchestrator must wait on the hook future (Future#get) before committing the
    // state.
    InOrder inOrder =
        inOrder(beforePreparationHook, beforePreparationHookFuture, coordinatorCommitHandler);
    inOrder.verify(beforePreparationHook).handle(context);
    inOrder.verify(beforePreparationHookFuture).get();
    inOrder.verify(coordinatorCommitHandler).commitState(context);
  }

  // =========================================================================
  // commit() — one-phase commit
  // =========================================================================

  @Test
  public void commit_OnePhaseCommitted_ShouldShortCircuitAndNotRunTwoPhaseFlow()
      throws CommitException, UnknownTransactionStatusException, CrudException,
          PreparationException {
    // Arrange — stub canOnePhaseCommit on the spied handler so we exercise the short-circuit.
    Snapshot snapshot = snapshotWithWrites();
    doReturn(true).when(handler).canOnePhaseCommit(any(TransactionContext.class));
    // Set a hook to confirm the one-phase fast path does not invoke it. (In production the hook and
    // the one-phase optimization are mutually exclusive by configuration, but pin the behavior.)
    doReturn(beforePreparationHookFuture).when(beforePreparationHook).handle(any());
    handler.setBeforePreparationHook(beforePreparationHook);
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.commit(context);

    // Assert
    verify(handler).canOnePhaseCommit(context);
    verify(handler).onePhaseCommitRecords(context);
    verify(participantCommitHandler, never()).prepareRecords(any());
    verify(coordinatorCommitHandler, never()).commitState(any());
    verify(beforePreparationHook, never()).handle(any());
  }

  @Test
  public void commit_OnePhaseCommitted_ThrowsUnknown_ShouldDelegateOnFailureBeforeCommit()
      throws CommitException, UnknownTransactionStatusException, CrudException {
    // Arrange
    Snapshot snapshot = snapshotWithWrites();
    doReturn(true).when(handler).canOnePhaseCommit(any(TransactionContext.class));
    doThrow(UnknownTransactionStatusException.class)
        .when(handler)
        .onePhaseCommitRecords(any(TransactionContext.class));
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatThrownBy(() -> handler.commit(context))
        .isInstanceOf(UnknownTransactionStatusException.class);

    verify(handler).onFailureBeforeCommit(context);
  }

  // =========================================================================
  // Pass-through delegation tests
  // =========================================================================

  @Test
  public void prepareRecords_ShouldDelegateToParticipantHandler() throws Exception {
    Snapshot snapshot = snapshotWithWrites();
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    handler.prepareRecords(context);

    verify(participantCommitHandler).prepareRecords(context);
  }

  @Test
  public void validateRecords_ShouldDelegateToParticipantHandler() throws Exception {
    Snapshot snapshot = snapshotWithWrites();
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    handler.validateRecords(context);

    verify(participantCommitHandler).validateRecords(context);
  }

  @Test
  public void commitRecords_ShouldDelegateToParticipantHandler() throws Exception {
    Snapshot snapshot = snapshotWithWrites();
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    handler.commitRecords(context);

    verify(participantCommitHandler).commitRecords(context);
  }

  @Test
  public void rollbackRecords_ShouldDelegateToParticipantHandler() throws Exception {
    Snapshot snapshot = snapshotWithWrites();
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    handler.rollbackRecords(context);

    verify(participantCommitHandler).rollbackRecords(context);
  }

  @Test
  public void canOnePhaseCommit_ShouldDelegateToParticipantHandler() throws Exception {
    Snapshot snapshot = snapshotWithWrites();
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    handler.canOnePhaseCommit(context);

    verify(participantCommitHandler).canOnePhaseCommit(context);
  }

  @Test
  public void onePhaseCommitRecords_ShouldDelegateToParticipantHandler() throws Exception {
    Snapshot snapshot = snapshotWithWrites();
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    handler.onePhaseCommitRecords(context);

    verify(participantCommitHandler).onePhaseCommitRecords(context);
  }

  @Test
  public void commitState_ShouldDelegateToCoordinatorHandler() throws Exception {
    Snapshot snapshot = snapshotWithWrites();
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    handler.commitState(context);

    verify(coordinatorCommitHandler).commitState(context);
  }

  @Test
  public void commitStateWithoutWriteSet_ShouldDelegateToCoordinatorHandler() throws Exception {
    Snapshot snapshot = snapshotWithoutWrites();
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    handler.commitStateWithoutWriteSet(context);

    verify(coordinatorCommitHandler).commitStateWithoutWriteSet(context);
  }

  @Test
  public void abortState_ShouldDelegateToCoordinatorHandler() throws Exception {
    Snapshot snapshot = snapshotWithWrites();
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    handler.abortState(context);

    verify(coordinatorCommitHandler).abortState(context);
  }

  @Test
  public void abortStateWithoutWriteSet_ShouldDelegateToCoordinatorHandler() throws Exception {
    handler.abortStateWithoutWriteSet(anyId());

    verify(coordinatorCommitHandler).abortStateWithoutWriteSet(anyId());
  }

  @Test
  public void forceAbortState_ShouldDelegateToCoordinatorHandler() throws Exception {
    handler.forceAbortState(anyId());

    verify(coordinatorCommitHandler).forceAbortState(anyId());
  }
}
