package com.scalar.db.common;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.GlobalTransaction;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Adapts a {@link DistributedTransaction} to the {@link GlobalTransaction} API.
 *
 * <p>This is the overall (global) handle of a single-phase-backed distributed transaction. It holds
 * the underlying {@link DistributedTransaction} only to drive commit/rollback; CRUD happens on the
 * {@link com.scalar.db.api.BranchTransaction} handles that join this transaction by its ID. {@link
 * #commit()} performs the single-phase commit; {@link #rollback()} rolls it back.
 *
 * <p>See {@link DistributedTransactionBackedGlobalTransactionManager} for how the manager is wired
 * and the global transaction and its branches are begun.
 */
public class DistributedTransactionBackedGlobalTransaction implements GlobalTransaction {

  private final DistributedTransaction transaction;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public DistributedTransactionBackedGlobalTransaction(DistributedTransaction transaction) {
    this.transaction = transaction;
  }

  @Override
  public String getId() {
    return transaction.getId();
  }

  @Override
  public void commit() throws CommitException, UnknownTransactionStatusException {
    transaction.commit();
  }

  @Override
  public void rollback() throws RollbackException {
    transaction.rollback();
  }
}
