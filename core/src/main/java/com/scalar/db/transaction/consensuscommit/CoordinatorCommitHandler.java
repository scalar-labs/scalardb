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
 * <p>Operates on a transaction id and a pre-encoded {@link WriteSet} (plus, for the commit path,
 * the {@code committedAt} to stamp); callers encode the write set before calling.
 *
 * <p>When {@code commitState} loses a putState race and the persisted state turns out to be ABORTED
 * (or absent), this handler reports a {@link CommitConflictException} and leaves the rollback of
 * the transaction's prepared records to the caller.
 */
@ThreadSafe
class CoordinatorCommitHandler {
  private static final Logger logger = LoggerFactory.getLogger(CoordinatorCommitHandler.class);

  private final CoordinatorStateAccessor coordinator;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  CoordinatorCommitHandler(CoordinatorStateAccessor coordinator) {
    this.coordinator = checkNotNull(coordinator);
  }

  /**
   * Writes the COMMITTED state for the transaction identified by {@code id}, persisting the given
   * pre-encoded {@code writeSet} (or none when {@code null}).
   *
   * @return the {@code committedAt} stamped on the COMMITTED Coordinator state row — the value this
   *     call generates, or, when this commit lost a putState race to an already-COMMITTED row, that
   *     row's committedAt. The caller stamps the committed data records with the returned value so
   *     the row and the records share one timestamp.
   */
  long commitState(String id, @Nullable WriteSet writeSet)
      throws CommitConflictException, UnknownTransactionStatusException {
    try {
      long committedAt = System.currentTimeMillis();
      CoordinatorStateAccessor.State state =
          new CoordinatorStateAccessor.State(id, writeSet, TransactionState.COMMITTED, committedAt);
      coordinator.putState(state);
      logger.debug("Transaction {} is committed successfully at {}", id, committedAt);
      return committedAt;
    } catch (CoordinatorConflictException e) {
      return handleCommitConflict(id, e);
    } catch (CoordinatorException e) {
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_UNKNOWN_COORDINATOR_STATUS.buildMessage(e.getMessage()),
          e,
          id);
    }
  }

  /**
   * Resolves a putState conflict. Returns the {@code committedAt} of the already-persisted
   * COMMITTED state when this transaction turns out to be already committed; otherwise reports the
   * conflict (the caller rolls the records back) by throwing {@link CommitConflictException}.
   */
  long handleCommitConflict(String id, Exception cause)
      throws CommitConflictException, UnknownTransactionStatusException {
    try {
      Optional<CoordinatorStateAccessor.State> s = coordinator.getState(id);
      if (s.isPresent()) {
        CoordinatorStateAccessor.State persisted = s.get();
        if (persisted.getState() == TransactionState.ABORTED) {
          throw new CommitConflictException(
              CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHEN_COMMITTING_STATE.buildMessage(
                  cause.getMessage()),
              cause,
              id);
        }
        // Otherwise the coordinator state is present and COMMITTED, which means this transaction
        // has already committed. No other actor writes this transaction's COMMITTED state, so the
        // row is this commit's own: an earlier attempt of the putIfNotExists was applied although
        // it was reported as failed (e.g., a write timeout or a lost response), and a retry of it
        // then lost the race to that row. The transaction is committed, so return the persisted
        // row's committedAt and let the caller commit the records with it (keeping the row and the
        // records on a single timestamp).

        return persisted.getCreatedAt();
      } else {
        // The coordinator state is absent: a row existed when our putIfNotExists lost the race, but
        // it is gone now. This means the conflicting row was an ABORTED written by a lazy recovery
        // (which also rolled the records back) and later removed by the Coordinator state cleanup
        // process, so the transaction is definitively aborted. Report a conflict (the orchestrator
        // rolls the records back) -- the same outcome as the present-ABORTED case above.
        //
        // A COMMITTED row can be ruled out here. The only COMMITTED row this conflict can have
        // been against is this commit's own, and a COMMITTED coordinator row is only ever removed
        // by finishTransaction (the periodic cleanup removes only ABORTED rows), which runs after
        // the transaction has terminated. Had the conflict been against it, it would still be
        // present and handled by the present-COMMITTED branch above.
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

        throw new CommitConflictException(
            CoreError.CONSENSUS_COMMIT_CONFLICT_OCCURRED_WHEN_COMMITTING_STATE.buildMessage(
                cause.getMessage()),
            cause,
            id);
      }
    } catch (CoordinatorException ex) {
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_CANNOT_GET_COORDINATOR_STATUS.buildMessage(ex.getMessage()),
          ex,
          id);
    }
  }

  /**
   * Writes the ABORTED state for the transaction identified by {@code id}, persisting the given
   * pre-encoded {@code writeSet} (or none when {@code null}).
   */
  TransactionState abortState(String id, @Nullable WriteSet writeSet)
      throws UnknownTransactionStatusException {
    try {
      CoordinatorStateAccessor.State state =
          new CoordinatorStateAccessor.State(
              id, writeSet, TransactionState.ABORTED, System.currentTimeMillis());
      coordinator.putState(state);
      return TransactionState.ABORTED;
    } catch (CoordinatorConflictException e) {
      // Resolves the final transaction state after our ABORTED putIfNotExists lost the race, on the
      // self-abort path: a commit orchestrator (CommitHandler or ConsensusCommitCoordinator)
      // aborting its own transaction after a prepare or validate failure, before commitState.
      // Follows the persisted state when present; an absent row is determinable as ABORTED here,
      // because the transaction provably never committed, so the conflicting row could only have
      // been a lazy-recovery ABORTED, later removed by the cleanup process.
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
   * CoordinatorStateAccessor#forceAbort} so the 2-step protocol wins against an in-flight normal
   * group commit when the id is a group-commit full key.
   */
  TransactionState forceAbortState(String id) throws UnknownTransactionStatusException {
    try {
      coordinator.forceAbort(id);
      return TransactionState.ABORTED;
    } catch (CoordinatorConflictException e) {
      // Resolves the final transaction state after our ABORTED putIfNotExists lost the race, on the
      // abort-by-id path (DistributedTransactionManager.rollback(String) / abort(String)). Follows
      // the persisted state when present. Unlike the self-abort above, an absent row here is
      // genuinely undeterminable: abort-by-id can target a transaction that actually committed, and
      // finishTransaction can remove that COMMITTED row, so an absent row may be a cleaned-up
      // COMMITTED rather than a cleaned-up ABORTED. Report an honest
      // UnknownTransactionStatusException preserving the original conflict, rather than fabricating
      // a terminal state.
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
      return coordinator.getState(id).map(CoordinatorStateAccessor.State::getState);
    } catch (CoordinatorException e1) {
      e1.addSuppressed(e);
      throw new UnknownTransactionStatusException(
          CoreError.CONSENSUS_COMMIT_CANNOT_GET_COORDINATOR_STATUS.buildMessage(e1.getMessage()),
          e1,
          id);
    }
  }
}
