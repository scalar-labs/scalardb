package com.scalar.db.common;

import com.scalar.db.api.GlobalTransaction;
import com.scalar.db.api.TwoPhaseCommitCoordinator;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Adapts a {@link TwoPhaseCommitCoordinator} to the {@link GlobalTransaction} API.
 *
 * <p>This is the overall (global) handle of a distributed transaction. It holds no data-store
 * resources itself; CRUD happens on the {@link com.scalar.db.api.BranchTransaction} handles that
 * join this transaction by its ID. {@link #commit()} drives the full two-phase commit across the
 * joined participants through the coordinator (which generates the prepared/committed timestamps
 * internally); {@link #rollback()} drives the coordinator's rollback.
 *
 * <p>See {@link TwoPhaseCommitBackedGlobalTransactionManager} for how the coordinator is wired and
 * the global transaction and its branches are begun.
 */
@NotThreadSafe
public class TwoPhaseCommitBackedGlobalTransaction implements GlobalTransaction {

  private final TwoPhaseCommitCoordinator coordinator;
  private final String transactionId;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public TwoPhaseCommitBackedGlobalTransaction(
      TwoPhaseCommitCoordinator coordinator, String transactionId) {
    this.coordinator = coordinator;
    this.transactionId = transactionId;
  }

  @Override
  public String getId() {
    return transactionId;
  }

  @Override
  public void commit() throws CommitException, UnknownTransactionStatusException {
    // Drive the full 2PC; the coordinator generates the prepared/committed timestamps internally.
    // The coordinator already reports prepare/validate failures as CommitConflictException /
    // CommitException, so those propagate as-is; only an unknown transaction needs translating.
    try {
      coordinator.commit(transactionId);
    } catch (TransactionNotFoundException e) {
      // The coordinator no longer knows this transaction (e.g., it expired). Surface it as a
      // retriable commit conflict so the caller can restart the transaction from the beginning.
      throw new CommitConflictException(e.getMessage(), e, transactionId);
    }
  }

  @Override
  public void rollback() throws RollbackException {
    coordinator.rollback(transactionId);
  }
}
