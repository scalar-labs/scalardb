package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Group-commit-aware orchestrator variant. The protocol flow is the same as {@link CommitHandler};
 * the responsibility added at this layer is releasing the group-commit slot reservation on the
 * various failure / cleanup paths so an aborted transaction does not sit in the group buffer
 * waiting for a sibling.
 *
 * <p>The actual group-commit emitter and slot-cancellation primitive live on {@link
 * CoordinatorCommitHandlerWithGroupCommit}; this orchestrator merely wires in the right
 * Coordinator-side handler and invokes the slot cancellation at the right points in the flow.
 */
@ThreadSafe
public class CommitHandlerWithGroupCommit extends CommitHandler {
  private final CoordinatorCommitHandlerWithGroupCommit coordinatorHandler;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public CommitHandlerWithGroupCommit(
      DistributedStorage storage,
      Coordinator coordinator,
      TransactionTableMetadataManager tableMetadataManager,
      ParallelExecutor parallelExecutor,
      MutationsGrouper mutationsGrouper,
      boolean coordinatorWriteOmissionOnReadOnlyEnabled,
      boolean coordinatorWriteSetLoggingEnabled,
      boolean onePhaseCommitEnabled,
      CoordinatorGroupCommitter groupCommitter) {
    this(
        tableMetadataManager,
        coordinatorWriteOmissionOnReadOnlyEnabled,
        coordinatorWriteSetLoggingEnabled,
        new ParticipantCommitHandler(
            storage,
            tableMetadataManager,
            parallelExecutor,
            mutationsGrouper,
            onePhaseCommitEnabled),
        coordinator,
        checkNotNull(groupCommitter));
  }

  // Second-stage delegating constructor that builds the two specialized handlers before handing
  // them to the package-private constructor.
  private CommitHandlerWithGroupCommit(
      TransactionTableMetadataManager tableMetadataManager,
      boolean coordinatorWriteOmissionOnReadOnlyEnabled,
      boolean coordinatorWriteSetLoggingEnabled,
      ParticipantCommitHandler participantCommitHandler,
      Coordinator coordinator,
      CoordinatorGroupCommitter groupCommitter) {
    this(
        coordinatorWriteOmissionOnReadOnlyEnabled,
        coordinatorWriteSetLoggingEnabled,
        new WriteSetEncoder(tableMetadataManager),
        new CoordinatorCommitHandlerWithGroupCommit(
            coordinator, groupCommitter, coordinatorWriteSetLoggingEnabled),
        participantCommitHandler);
  }

  // Package-private so test code can inject Mockito spies of the two specialized handlers via
  // constructor injection rather than via reflection.
  @SuppressFBWarnings("EI_EXPOSE_REP2")
  CommitHandlerWithGroupCommit(
      boolean coordinatorWriteOmissionOnReadOnlyEnabled,
      boolean coordinatorWriteSetLoggingEnabled,
      WriteSetEncoder writeSetEncoder,
      CoordinatorCommitHandlerWithGroupCommit coordinatorHandler,
      ParticipantCommitHandler participantCommitHandler) {
    super(
        coordinatorWriteOmissionOnReadOnlyEnabled,
        coordinatorWriteSetLoggingEnabled,
        writeSetEncoder,
        coordinatorHandler,
        participantCommitHandler);
    this.coordinatorHandler = coordinatorHandler;
  }

  @Override
  public void commit(TransactionContext context)
      throws CommitException, UnknownTransactionStatusException {
    cancelGroupCommitIfCoordinatorStateOmitted(context);
    super.commit(context);
  }

  /**
   * Releases the group-commit slot reservation when the orchestrator will not write a Coordinator
   * state row through the normal 2-phase commit path: a non-read-only transaction (which reserved a
   * slot) that turns out to have no writes/deletes while coordinator-write-omission is enabled, so
   * {@link CommitHandler#commit} skips {@code commitState} and the slot would otherwise sit unused.
   */
  private void cancelGroupCommitIfCoordinatorStateOmitted(TransactionContext context) {
    if (!context.readOnly
        && !context.snapshot.hasWritesOrDeletes()
        && coordinatorWriteOmissionOnReadOnlyEnabled) {
      cancelGroupCommitIfSlotReserved(context);
    }
  }

  private void cancelGroupCommitIfSlotReserved(TransactionContext context) {
    // A transaction only holds a group commit slot when one was reserved at begin time (e.g., a
    // read-only transaction does not reserve a slot when coordinator write omission is enabled).
    // When no slot was reserved there is nothing to release.
    if (!context.groupCommitSlotReserved) {
      return;
    }

    // FIXME: When a slot was reserved (so the early return above did not fire), this can run more
    // than once on a single failure path, because several cleanup callbacks each release the slot:
    //   - onFailureBeforeCommit() runs on the pre-commit-state failure paths (via
    //     safelyCallOnFailureBeforeCommit);
    //   - abortState() additionally runs from abortStateAndRollbackRecordsIfNeeded() when the
    //     transaction has writes/deletes, or coordinator write omission on read-only is disabled;
    //   - onePhaseCommitRecords() pre-cancels the slot before its body, so a one-phase-commit
    //     failure followed by onFailureBeforeCommit() also cancels twice.
    // It is currently safe only because groupCommitter.remove() is idempotent. The cleanup paths
    // should be reworked so the slot is released exactly once instead of relying on that
    // idempotency.
    coordinatorHandler.cancelGroupCommit(context.transactionId);
  }

  @Override
  boolean canOnePhaseCommit(TransactionContext context) throws CommitException {
    try {
      return super.canOnePhaseCommit(context);
    } catch (CommitException e) {
      cancelGroupCommitIfSlotReserved(context);
      throw e;
    }
  }

  @Override
  void onePhaseCommitRecords(TransactionContext context)
      throws CommitConflictException, UnknownTransactionStatusException {
    cancelGroupCommitIfSlotReserved(context);
    super.onePhaseCommitRecords(context);
  }

  @Override
  protected void onFailureBeforeCommit(TransactionContext context) {
    cancelGroupCommitIfSlotReserved(context);
  }

  @Override
  public TransactionState abortState(TransactionContext context)
      throws UnknownTransactionStatusException {
    // Release the group-commit slot before writing the ABORTED state so it does not sit in the
    // group buffer waiting for a sibling.
    cancelGroupCommitIfSlotReserved(context);
    return super.abortState(context);
  }
}
