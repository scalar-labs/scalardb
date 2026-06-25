package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.db.api.TransactionState;
import com.scalar.db.common.CoreError;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.transaction.consensuscommit.proto.v1.WriteSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the Coordinator-side (state-table) operations of the Consensus Commit protocol: writing
 * the COMMITTED / ABORTED records to the Coordinator state table and resolving putState conflicts.
 *
 * <p>Methods here only touch the Coordinator state table via {@link Coordinator}; they never touch
 * user data tables. When {@code commitState} loses a putState race and the persisted state turns
 * out to be ABORTED (or absent), this handler reports a {@link CommitConflictException} and leaves
 * the rollback of the transaction's prepared records to the caller. This keeps the handler free of
 * any participant-side dependency.
 */
@ThreadSafe
class CoordinatorCommitHandler {
  private static final Logger logger = LoggerFactory.getLogger(CoordinatorCommitHandler.class);

  private final Coordinator coordinator;
  private final WriteSetEncoder writeSetEncoder;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  CoordinatorCommitHandler(Coordinator coordinator, WriteSetEncoder writeSetEncoder) {
    this.coordinator = checkNotNull(coordinator);
    this.writeSetEncoder = checkNotNull(writeSetEncoder);
  }

  /**
   * Writes the COMMITTED state with the {@code tx_write_set} populated from the given context's
   * snapshot.
   */
  void commitState(TransactionContext context)
      throws CommitConflictException, UnknownTransactionStatusException {
    commitStateInternal(context, writeSetEncoder.encodeSingleGroupWriteSet(context, false));
  }

  /** Writes the COMMITTED state without persisting a {@code tx_write_set}. */
  void commitStateWithoutWriteSet(TransactionContext context)
      throws CommitConflictException, UnknownTransactionStatusException {
    commitStateInternal(context, null);
  }

  private void commitStateInternal(TransactionContext context, @Nullable WriteSet writeSet)
      throws CommitConflictException, UnknownTransactionStatusException {
    String id = context.transactionId;
    try {
      Coordinator.State state =
          new Coordinator.State(
              id, writeSet, TransactionState.COMMITTED, System.currentTimeMillis());
      coordinator.putState(state);
      logger.debug(
          "Transaction {} is committed successfully at {}", id, System.currentTimeMillis());
    } catch (CoordinatorConflictException e) {
      handleCommitConflict(context, e);
    } catch (CoordinatorException e) {
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_UNKNOWN_COORDINATOR_STATUS.buildMessage(e.getMessage()),
          e,
          id);
    }
  }

  void handleCommitConflict(TransactionContext context, Exception cause)
      throws CommitConflictException, UnknownTransactionStatusException {
    try {
      Optional<Coordinator.State> s = coordinator.getState(context.transactionId);
      if (s.isPresent()) {
        TransactionState state = s.get().getState();
        if (state == TransactionState.ABORTED) {
          throw new CommitConflictException(
              CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHEN_COMMITTING_STATE.buildMessage(
                  cause.getMessage()),
              cause,
              context.transactionId);
        }
        // Otherwise the coordinator state is present and COMMITTED, which means this transaction
        // has already committed. Only Two-phase Commit I/F reaches this branch: there the same
        // transaction's commit can be driven more than once -- re-invoked for recovery, or
        // committed by multiple participants -- so an earlier or concurrent commit of this
        // transaction may have already written its COMMITTED state, and this commit then loses the
        // putIfNotExists race and observes it here. With One-phase Commit I/F this is unreachable:
        // this commit is the only writer of the COMMITTED state and it just lost the race, so the
        // conflicting row was an ABORTED from a lazy recovery, handled above. The transaction is
        // committed, so return normally and let the caller commit the records.
        //
        // TODO: revisit this if/when the Two-phase Commit I/F is removed -- it would then be
        // unreachable (a COMMITTED state could never be observed after a conflict here).
      } else {
        // The coordinator state is absent: a row existed when our putIfNotExists lost the race, but
        // it is gone now. In both interfaces this means the conflicting row was an ABORTED written
        // by a lazy recovery (which also rolled the records back) and later removed by the
        // Coordinator state cleanup process, so the transaction is definitively aborted. Report a
        // conflict (the orchestrator rolls the records back) -- the same outcome as the
        // present-ABORTED case above.
        //
        // A COMMITTED row can be ruled out here in both interfaces:
        //   - One-phase Commit I/F: this commit is the only writer of this transaction's COMMITTED
        //     state, and it just lost the race, so the conflicting row could only have been an
        //     ABORTED from a lazy recovery -- a COMMITTED for this transaction never existed.
        //   - Two-phase Commit I/F: other participants or re-driven commits of the same transaction
        //     can also write COMMITTED, so the conflict could in principle have been against a
        //     COMMITTED row. But the Two-phase Commit I/F does not assume finishTransaction,
        //     and a COMMITTED coordinator row is only ever removed by finishTransaction (the
        //     periodic cleanup removes only ABORTED rows). So a COMMITTED row never disappears
        //     here: had the conflict been against one, it would still be present and handled by
        //     the present-COMMITTED branch above. An absent row therefore means the conflict was
        //     an ABORTED.
        //
        // Group commit reaches an absent state by a second, more common route, and the outcome is
        // still correct. There the conflicting putIfNotExists is keyed on the parent ID (the group
        // emitter writes the parent-ID COMMITTED row), so the row it conflicts with -- a lazy
        // recovery's empty-tx_child_ids ABORTED parent row -- can still be present. getState then
        // resolves by full ID: it finds the parent row, sees it does not list this child, and finds
        // no full-ID row, so it returns empty. That empty does not mean a cleaned-up row; it means
        // this child was never committed (a committed child would either be listed in the parent
        // row or have its own full-ID COMMITTED row, and getState would return it). Rolling back
        // and reporting a retryable conflict is the correct outcome for such an uncommitted child.
        //
        // TODO: revisit this if/when the Two-phase Commit I/F is removed.

        throw new CommitConflictException(
            CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHEN_COMMITTING_STATE.buildMessage(
                cause.getMessage()),
            cause,
            context.transactionId);
      }
    } catch (CoordinatorException ex) {
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_CANNOT_GET_COORDINATOR_STATUS.buildMessage(ex.getMessage()),
          ex,
          context.transactionId);
    }
  }

  /**
   * Writes the ABORTED state with the {@code tx_write_set} populated from the given context's
   * snapshot.
   */
  TransactionState abortState(TransactionContext context) throws UnknownTransactionStatusException {
    return abortStateInternal(
        context.transactionId, writeSetEncoder.encodeSingleGroupWriteSet(context, false));
  }

  /** Writes the ABORTED state without persisting a {@code tx_write_set} via a single putState. */
  TransactionState abortStateWithoutWriteSet(String id) throws UnknownTransactionStatusException {
    return abortStateInternal(id, null);
  }

  private TransactionState abortStateInternal(String id, @Nullable WriteSet writeSet)
      throws UnknownTransactionStatusException {
    try {
      Coordinator.State state =
          new Coordinator.State(id, writeSet, TransactionState.ABORTED, System.currentTimeMillis());
      coordinator.putState(state);
      return TransactionState.ABORTED;
    } catch (CoordinatorConflictException e) {
      // Resolves the final transaction state after our ABORTED putIfNotExists lost the race, for
      // the self-abort path and all Two-phase Commit I/F aborts. Follows the persisted state when
      // present; an absent row is determinable as ABORTED here:
      //   - One-phase Commit I/F self-abort (abortState from a commit-path failure, before
      //     commitState runs): the transaction provably never committed, so the conflicting row
      //     could only have been a lazy-recovery ABORTED, later removed by the cleanup process.
      //   - Two-phase Commit I/F (rollback and abort-by-id): other participants can write
      //     COMMITTED, but the Two-phase Commit I/F does not assume finishTransaction, and a
      //     COMMITTED coordinator row is only ever removed by finishTransaction (the periodic
      //     cleanup removes only ABORTED rows). So a COMMITTED never disappears: had the conflict
      //     been against one, it would still be present and returned above. An absent row therefore
      //     means the conflict was an ABORTED.
      //
      // TODO: revisit this if/when the Two-phase Commit I/F is removed

      return readCoordinatorStateAfterAbortConflict(id, e).orElse(TransactionState.ABORTED);
    } catch (CoordinatorException e) {
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_UNKNOWN_COORDINATOR_STATUS.buildMessage(e.getMessage()),
          e,
          id);
    }
  }

  /**
   * Writes the ABORTED state for a manager-level rollback/abort by transaction ID. Uses {@link
   * Coordinator#forceAbort} so the 2-step protocol wins against an in-flight normal group commit
   * when the id is a group-commit full key.
   */
  TransactionState forceAbortState(String id) throws UnknownTransactionStatusException {
    try {
      coordinator.forceAbort(id);
      return TransactionState.ABORTED;
    } catch (CoordinatorConflictException e) {
      // Resolves the final transaction state after our ABORTED putIfNotExists lost the race, for
      // the One-phase Commit I/F abort-by-id path (DistributedTransactionManager.rollback(String) /
      // abort(String)). Follows the persisted state when present. Unlike the self-abort and
      // Two-phase Commit I/F aborts, an absent row here is genuinely undeterminable: abort-by-id
      // can target a transaction that actually committed, and in One-phase Commit I/F
      // finishTransaction can remove that COMMITTED row, so an absent row may be a cleaned-up
      // COMMITTED rather than a cleaned-up ABORTED. Report an honest
      // UnknownTransactionStatusException preserving the original conflict, rather than fabricating
      // a terminal state.
      //
      // TODO: revisit this if/when the Two-phase Commit I/F is removed

      Optional<TransactionState> persisted = readCoordinatorStateAfterAbortConflict(id, e);
      if (persisted.isPresent()) {
        return persisted.get();
      }
      throw new UnknownTransactionStatusException(
          CoreError
              .CONSENSUS_COMMIT_ABORTING_STATE_FAILED_WITH_NO_MUTATION_EXCEPTION_BUT_COORDINATOR_STATUS_DOES_NOT_EXIST
              .buildMessage(e.getMessage()),
          e,
          id);
    } catch (CoordinatorException e) {
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_UNKNOWN_COORDINATOR_STATUS.buildMessage(e.getMessage()),
          e,
          id);
    }
  }

  /**
   * Reads the persisted coordinator state after our ABORTED putIfNotExists lost the race. Returns
   * the already-persisted COMMITTED/ABORTED state when present, or empty when the row is absent
   * (already removed by the Coordinator state cleanup process). The two callers interpret an absent
   * row differently, because whether it is determinable depends on the calling path. A coordinator
   * read failure is undeterminable regardless of path, so it surfaces as
   * UnknownTransactionStatusException with the original conflict preserved as the cause.
   */
  private Optional<TransactionState> readCoordinatorStateAfterAbortConflict(
      String id, CoordinatorConflictException e) throws UnknownTransactionStatusException {
    try {
      return coordinator.getState(id).map(Coordinator.State::getState);
    } catch (CoordinatorException e1) {
      e1.addSuppressed(e);
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_CANNOT_GET_COORDINATOR_STATUS.buildMessage(e1.getMessage()),
          e1,
          id);
    }
  }
}
