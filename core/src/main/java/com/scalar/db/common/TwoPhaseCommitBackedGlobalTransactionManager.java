package com.scalar.db.common;

import com.scalar.db.api.BranchTransaction;
import com.scalar.db.api.GlobalTransaction;
import com.scalar.db.api.GlobalTransactionManager;
import com.scalar.db.api.TwoPhaseCommitCoordinator;
import com.scalar.db.api.TwoPhaseCommitParticipant;
import com.scalar.db.exception.transaction.TransactionException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Adapts a {@link TwoPhaseCommitCoordinator} and a single in-process {@link
 * TwoPhaseCommitParticipant} to the {@link GlobalTransactionManager} API.
 *
 * <p>The coordinator/participant split maps directly onto the global/branch roles:
 *
 * <ul>
 *   <li>{@code begin} allocates a new distributed transaction via the coordinator and returns a
 *       {@link TwoPhaseCommitBackedGlobalTransaction} — the overall handle used to drive
 *       commit/rollback. The transaction begins with no participants.
 *   <li>{@code beginBranch} enlists the in-process participant in the transaction for the given
 *       global transaction ID and returns a {@link TwoPhaseCommitBackedBranchTransaction} — the
 *       CRUD handle for that branch.
 * </ul>
 *
 * <p>The per-branch {@code attributes} passed to {@code beginBranch} are propagated client-side
 * into each CRUD operation issued on the branch (via {@link
 * AttributePropagatingBranchTransaction}). The {@code readOnly} flag and the transaction-scoped
 * attributes supplied to {@code begin} are forwarded to the participant when the coordinator
 * establishes its local context.
 *
 * <p>A single in-process participant is wired in, so a global transaction has at most one
 * meaningful branch (enlisting is idempotent per participant ID). Calling {@code beginBranch} again
 * for the same transaction returns a new handle fronting that same participant context — the {@link
 * BranchTransaction#end()} bookkeeping is per handle — so begin each branch once and drive it
 * through that one handle. The participant may be {@code null} (coordinator-only), in which case
 * {@code beginBranch} is unsupported.
 */
@ThreadSafe
public class TwoPhaseCommitBackedGlobalTransactionManager implements GlobalTransactionManager {

  private final TwoPhaseCommitCoordinator coordinator;
  @Nullable private final TwoPhaseCommitParticipant participant;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public TwoPhaseCommitBackedGlobalTransactionManager(
      TwoPhaseCommitCoordinator coordinator, @Nullable TwoPhaseCommitParticipant participant) {
    this.coordinator = coordinator;
    this.participant = participant;
  }

  @Override
  public GlobalTransaction begin(Map<String, String> attributes) throws TransactionException {
    return beginInternal(false, attributes);
  }

  @Override
  public GlobalTransaction beginReadOnly(Map<String, String> attributes)
      throws TransactionException {
    return beginInternal(true, attributes);
  }

  private GlobalTransaction beginInternal(boolean readOnly, Map<String, String> attributes)
      throws TransactionException {
    String canonicalId = coordinator.begin(null, readOnly, attributes);
    return new TwoPhaseCommitBackedGlobalTransaction(coordinator, canonicalId);
  }

  @Override
  public BranchTransaction beginBranch(String transactionId, Map<String, String> attributes)
      throws TransactionException {
    if (participant == null) {
      throw new UnsupportedOperationException(
          CoreError.COORDINATOR_ONLY_GLOBAL_TRANSACTION_MANAGER_BRANCH_NOT_SUPPORTED
              .buildMessage());
    }
    // Enlist the in-process participant in the global transaction. enlist establishes the
    // participant's local context, forwarding the readOnly flag and the transaction-scoped
    // attributes supplied at begin. The per-branch attributes passed here are propagated
    // client-side into each CRUD operation by AttributePropagatingBranchTransaction.
    coordinator.enlist(transactionId, participant);
    BranchTransaction branch =
        new TwoPhaseCommitBackedBranchTransaction(participant, transactionId);
    return attributes.isEmpty()
        ? branch
        : new AttributePropagatingBranchTransaction(branch, attributes);
  }

  @Override
  public void close() {
    try {
      coordinator.close();
    } finally {
      if (participant != null) {
        participant.close();
      }
    }
  }
}
