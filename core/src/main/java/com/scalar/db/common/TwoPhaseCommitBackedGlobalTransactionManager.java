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

/**
 * Adapts a {@link TwoPhaseCommitCoordinator} and a single in-process {@link
 * TwoPhaseCommitParticipant} to the {@link GlobalTransactionManager} API.
 *
 * <p>The coordinator/participant split maps directly onto the global/branch roles:
 *
 * <ul>
 *   <li>{@code beginGlobal} allocates a new distributed transaction via the coordinator and returns
 *       a {@link TwoPhaseCommitBackedGlobalTransaction} — the overall handle used to drive
 *       commit/rollback. The transaction begins with no participants.
 *   <li>{@code beginBranch} joins the in-process participant to the transaction for the given
 *       global transaction ID and returns a {@link TwoPhaseCommitBackedBranchTransaction} — the
 *       CRUD handle for that branch.
 * </ul>
 *
 * <p>The per-branch {@code attributes} passed to {@code beginBranch} are propagated client-side
 * into each CRUD operation issued on the branch (via {@link
 * AttributePropagatingBranchTransaction}). The {@code readOnly} flag and the transaction-scoped
 * attributes supplied to {@code beginGlobal} are forwarded to the participant when the coordinator
 * establishes its local context.
 *
 * <p>A single in-process participant is wired in, so a global transaction has at most one
 * meaningful branch (joining is idempotent per participant ID). The participant may be {@code null}
 * (coordinator-only), in which case {@code beginBranch} is unsupported.
 */
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
  public GlobalTransaction beginGlobal(Map<String, String> attributes) throws TransactionException {
    return beginGlobalInternal(false, attributes);
  }

  @Override
  public GlobalTransaction beginGlobalReadOnly(Map<String, String> attributes)
      throws TransactionException {
    return beginGlobalInternal(true, attributes);
  }

  private GlobalTransaction beginGlobalInternal(boolean readOnly, Map<String, String> attributes)
      throws TransactionException {
    String canonicalId = coordinator.begin(null, readOnly, attributes);
    return new TwoPhaseCommitBackedGlobalTransaction(coordinator, canonicalId);
  }

  @Override
  public BranchTransaction beginBranch(String transactionId, Map<String, String> attributes)
      throws TransactionException {
    if (participant == null) {
      throw new UnsupportedOperationException(
          "Branches are not supported by this coordinator-only global transaction manager because no"
              + " participant is configured");
    }
    // Join the in-process participant to the global transaction. joinParticipant establishes the
    // participant's local context, forwarding the readOnly flag and the transaction-scoped
    // attributes supplied at beginGlobal. The per-branch attributes passed here are propagated
    // client-side into each CRUD operation by AttributePropagatingBranchTransaction.
    coordinator.joinParticipant(transactionId, participant);
    BranchTransaction branch =
        new TwoPhaseCommitBackedBranchTransaction(participant, transactionId);
    return attributes.isEmpty()
        ? branch
        : new AttributePropagatingBranchTransaction(branch, attributes);
  }

  @Override
  public void close() {
    coordinator.close();
    if (participant != null) {
      participant.close();
    }
  }
}
