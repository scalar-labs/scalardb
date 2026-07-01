package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

/**
 * Tests for {@link CommitHandlerWithGroupCommit}. Inherits the orchestration tests from {@link
 * CommitHandlerTest} (which pin which participant / coordinator handler methods are called for each
 * branch of {@code commit()}). The tests added here layer the group-commit-specific assertions on
 * top -- specifically, when the orchestrator delegates {@code cancelGroupCommit} to the
 * group-commit coordinator handler.
 *
 * <p>Both {@code participantCommitHandler} and the group-commit coordinator handler are mocks; the
 * underlying group committer is never instantiated.
 */
class CommitHandlerWithGroupCommitTest extends CommitHandlerTest {
  // Subclass-typed mock that doubles as the parent's coordinatorCommitHandler. Verifying
  // cancelGroupCommit requires the subclass type.
  private CoordinatorCommitHandlerWithGroupCommit groupCommitCoordinatorHandler;

  @Override
  protected TransactionContext createTransactionContext(
      String id, Snapshot snapshot, Isolation isolation, boolean readOnly, boolean oneOperation) {
    // In the group commit tests a slot is reserved for the transaction (the orchestrator's
    // group-commit-specific paths inspect this flag), so mark the context accordingly.
    return new TransactionContext(
        id, snapshot, isolation, readOnly, oneOperation, /* groupCommitSlotReserved= */ true);
  }

  @Override
  protected CommitHandler createCommitHandler(boolean coordinatorWriteOmissionOnReadOnlyEnabled) {
    groupCommitCoordinatorHandler = mock(CoordinatorCommitHandlerWithGroupCommit.class);
    coordinatorCommitHandler = groupCommitCoordinatorHandler;
    return new CommitHandlerWithGroupCommit(
        coordinatorWriteOmissionOnReadOnlyEnabled,
        /* coordinatorWriteSetLoggingEnabled= */ true,
        writeSetEncoder,
        groupCommitCoordinatorHandler,
        participantCommitHandler);
  }

  // The commitState delegation hooks from CommitHandlerTest apply as-is: the group commit handler
  // overrides the base CoordinatorCommitHandler#commitState(id, writeSet), so the inherited
  // assertions (which target that signature on the coordinatorCommitHandler mock) already exercise
  // the group-commit path.

  // =========================================================================
  // Group-commit-specific assertions
  //
  // Inherited tests cover which participant / coordinator handler methods are called for each
  // branch of commit(); the tests below add slot-cancel assertions specific to the override.
  // =========================================================================

  @Test
  public void commit_WithWrites_ShouldNotEarlyCancelGroupCommit() throws Exception {
    // For a normal (writes) commit the slot is consumed by commitState via groupCommitter.ready,
    // so the orchestrator's early-cancel optimization does NOT fire.
    // Arrange
    Snapshot snapshot = snapshotWithWrites();
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.commit(context);

    // Assert
    verify(groupCommitCoordinatorHandler, never()).cancelGroupCommit(any());
  }

  @Test
  public void commit_NonReadOnlyNoWritesOmissionEnabled_ShouldEarlyCancelGroupCommit()
      throws Exception {
    // The reserved slot is NOT consumed by the commit (commitState is skipped because of write
    // omission), so cancelGroupCommitIfCoordinatorStateOmitted delegates the cancel eagerly.
    // Arrange
    Snapshot snapshot = snapshotWithoutWrites();
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.commit(context);

    // Assert
    verify(groupCommitCoordinatorHandler).cancelGroupCommit(context.transactionId);
  }

  @Test
  public void commit_NonReadOnlyNoWritesOmissionDisabled_ShouldNotEarlyCancelGroupCommit()
      throws Exception {
    // With omission disabled, commitState still runs and needs the slot, so the early-cancel must
    // not fire.
    // Arrange
    handler = spy(createCommitHandler(/* coordinatorWriteOmissionOnReadOnlyEnabled= */ false));
    Snapshot snapshot = snapshotWithoutWrites();
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.commit(context);

    // Assert
    verify(groupCommitCoordinatorHandler, never()).cancelGroupCommit(any());
  }

  @Test
  public void commit_OnFailure_ShouldDelegateCancelGroupCommitViaOnFailureBeforeCommit()
      throws Exception {
    // The orchestrator's onFailureBeforeCommit override delegates to
    // coordinatorHandler.cancelGroupCommit.
    // Arrange
    Snapshot snapshot = snapshotWithoutWrites();
    doThrow(new RuntimeException("Something is wrong")).when(beforePreparationHook).handle(any());
    handler.setBeforePreparationHook(beforePreparationHook);
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, true, false);

    // Act Assert
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    verify(groupCommitCoordinatorHandler).cancelGroupCommit(context.transactionId);
  }

  @Test
  public void
      commit_FailingBeforePreparationHookGiven_BareTransactionId_ShouldNotTouchGroupCommitter()
          throws Exception {
    // With coordinator write omission enabled, ConsensusCommitManager.begin() does not reserve a
    // group commit slot for a read-only transaction, so the context's groupCommitSlotReserved
    // stays false. When the before-preparation hook fails, commit() must still fail with
    // CommitException, and the orchestrator must not touch the group committer since no slot was
    // reserved.
    // Arrange
    Snapshot snapshot = snapshotWithoutWrites();
    doThrow(new RuntimeException("Something is wrong")).when(beforePreparationHook).handle(any());
    handler.setBeforePreparationHook(beforePreparationHook);
    // Override createTransactionContext's default reserved=true with reserved=false here.
    TransactionContext context =
        new TransactionContext(
            anyId(),
            snapshot,
            Isolation.SNAPSHOT,
            /* readOnly= */ true,
            /* oneOperation= */ false,
            /* groupCommitSlotReserved= */ false);

    // Act Assert
    assertThatThrownBy(() -> handler.commit(context)).isInstanceOf(CommitException.class);

    verify(groupCommitCoordinatorHandler, never()).cancelGroupCommit(any());
  }

  // =========================================================================
  // Override-specific behaviour for abortState / canOnePhaseCommit / onePhaseCommitRecords
  // =========================================================================

  @Test
  public void abortState_ShouldCancelSlotBeforeDelegating() throws Exception {
    // The group-commit override releases the reserved slot before writing the ABORTED state, so an
    // aborted transaction does not sit in the group buffer waiting for a sibling.
    // Arrange
    Snapshot snapshot = snapshotWithWrites();
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);

    // Act
    handler.abortState(context);

    // Assert
    InOrder inOrder = inOrder(groupCommitCoordinatorHandler);
    inOrder.verify(groupCommitCoordinatorHandler).cancelGroupCommit(context.transactionId);
    inOrder.verify(groupCommitCoordinatorHandler).abortState(eq(context.transactionId), any());
  }

  @Test
  public void onePhaseCommitRecords_ShouldCancelSlotBeforeDelegating() throws Exception {
    // The group-commit override releases the reserved slot before delegating to
    // super.onePhaseCommitRecords, so the storage mutation runs outside the group buffer.
    // Arrange
    Snapshot snapshot = snapshotWithWrites();
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);
    doNothing().when(participantCommitHandler).onePhaseCommitRecords(context);

    // Act
    handler.onePhaseCommitRecords(context);

    // Assert
    InOrder inOrder = inOrder(groupCommitCoordinatorHandler, participantCommitHandler);
    inOrder.verify(groupCommitCoordinatorHandler).cancelGroupCommit(context.transactionId);
    inOrder.verify(participantCommitHandler).onePhaseCommitRecords(context);
  }

  @Test
  public void onePhaseCommitRecords_ShouldCancelSlotEvenWhenDelegateThrows() throws Exception {
    // The slot is released before delegating, so the storage mutation throwing afterwards still
    // leaves the slot released (cancel-before-delegate is unconditional).
    // Arrange
    Snapshot snapshot = snapshotWithWrites();
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);
    UnknownTransactionStatusException cause =
        new UnknownTransactionStatusException("storage failure", anyId());
    doThrow(cause).when(participantCommitHandler).onePhaseCommitRecords(context);

    // Act Assert
    assertThatThrownBy(() -> handler.onePhaseCommitRecords(context)).isSameAs(cause);

    verify(groupCommitCoordinatorHandler).cancelGroupCommit(context.transactionId);
  }

  @Test
  public void canOnePhaseCommit_WhenSuperThrows_ShouldCancelSlotAndRethrow() throws Exception {
    // The group-commit override of canOnePhaseCommit cancels the slot on any CommitException from
    // super (e.g., MutationsGrouper throws ExecutionException → wrapped as CommitException) so the
    // slot does not stay reserved when one-phase commit will not be attempted.
    // Arrange
    Snapshot snapshot = snapshotWithWrites();
    TransactionContext context =
        createTransactionContext(anyId(), snapshot, Isolation.SNAPSHOT, false, false);
    CommitException cause = new CommitException("grouper failure", anyId());
    doThrow(cause).when(participantCommitHandler).canOnePhaseCommit(context);

    // Act Assert
    assertThatThrownBy(() -> handler.canOnePhaseCommit(context)).isSameAs(cause);

    verify(groupCommitCoordinatorHandler).cancelGroupCommit(context.transactionId);
  }

  // We replace the snapshot mock builders to also wire up the participant's hasWritesOrDeletes
  // view, since the orchestrator hits the snapshot through context only (already covered by the
  // parent mock helpers).
  @Override
  protected Snapshot snapshotWithWrites() {
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.hasWritesOrDeletes()).thenReturn(true);
    return snapshot;
  }

  @Override
  protected Snapshot snapshotWithoutWrites() {
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.hasWritesOrDeletes()).thenReturn(false);
    return snapshot;
  }
}
