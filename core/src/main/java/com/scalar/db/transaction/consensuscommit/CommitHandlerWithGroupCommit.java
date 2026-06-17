package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.db.api.DistributedStorage;
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
      boolean onePhaseCommitEnabled,
      CoordinatorGroupCommitter groupCommitter) {
    this(
        tableMetadataManager,
        coordinatorWriteOmissionOnReadOnlyEnabled,
        new ParticipantCommitHandler(
            storage,
            tableMetadataManager,
            parallelExecutor,
            mutationsGrouper,
            onePhaseCommitEnabled),
        coordinator,
        checkNotNull(groupCommitter));
  }

  // Second-stage delegating constructor so the ParticipantCommitHandler is available when we build
  // the CoordinatorCommitHandlerWithGroupCommit.
  private CommitHandlerWithGroupCommit(
      TransactionTableMetadataManager tableMetadataManager,
      boolean coordinatorWriteOmissionOnReadOnlyEnabled,
      ParticipantCommitHandler participantCommitHandler,
      Coordinator coordinator,
      CoordinatorGroupCommitter groupCommitter) {
    this(
        coordinatorWriteOmissionOnReadOnlyEnabled,
        participantCommitHandler,
        new CoordinatorCommitHandlerWithGroupCommit(
            coordinator,
            new WriteSetEncoder(tableMetadataManager),
            participantCommitHandler,
            groupCommitter));
  }

  // Package-private so test code can inject Mockito spies of ParticipantCommitHandler /
  // CoordinatorCommitHandlerWithGroupCommit via constructor injection rather than via reflection.
  @SuppressFBWarnings("EI_EXPOSE_REP2")
  CommitHandlerWithGroupCommit(
      boolean coordinatorWriteOmissionOnReadOnlyEnabled,
      ParticipantCommitHandler participantCommitHandler,
      CoordinatorCommitHandlerWithGroupCommit coordinatorHandler) {
    super(coordinatorWriteOmissionOnReadOnlyEnabled, participantCommitHandler, coordinatorHandler);
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
   * state row through the normal 2-phase commit path. That happens when the transaction was begun
   * as non-read-only (and therefore reserved a slot) but turned out to have no writes/deletes,
   * combined with coordinator-write-omission being enabled — in which case {@link
   * CommitHandler#commit} skips {@code commitState} entirely and the reserved slot would otherwise
   * sit unused for the rest of the commit flow.
   *
   * <p>The other places that call {@link
   * CoordinatorCommitHandlerWithGroupCommit#cancelGroupCommitIfNeeded} (one-phase commit,
   * abort/conflict paths, before-commit failure cleanup) cannot share this predicate: at those
   * sites the transaction has writes/deletes, so the predicate would block the cancellation that
   * those paths actually need.
   */
  private void cancelGroupCommitIfCoordinatorStateOmitted(TransactionContext context) {
    if (!context.readOnly
        && !context.snapshot.hasWritesOrDeletes()
        && coordinatorWriteOmissionOnReadOnlyEnabled) {
      coordinatorHandler.cancelGroupCommitIfNeeded(context);
    }
  }

  @Override
  boolean canOnePhaseCommit(TransactionContext context) throws CommitException {
    try {
      return super.canOnePhaseCommit(context);
    } catch (CommitException e) {
      coordinatorHandler.cancelGroupCommitIfNeeded(context);
      throw e;
    }
  }

  @Override
  void onePhaseCommitRecords(TransactionContext context)
      throws CommitConflictException, UnknownTransactionStatusException {
    coordinatorHandler.cancelGroupCommitIfNeeded(context);
    super.onePhaseCommitRecords(context);
  }

  @Override
  protected void onFailureBeforeCommit(TransactionContext context) {
    coordinatorHandler.cancelGroupCommitIfNeeded(context);
  }
}
