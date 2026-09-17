package com.scalar.db.common;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.GlobalTransaction;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Adapts a {@link DistributedTransaction} to the {@link GlobalTransaction} API.
 *
 * <p>{@link #commit()} performs the single-phase commit of the shared underlying transaction;
 * {@link #rollback()} rolls it back. The handle holds the underlying {@link DistributedTransaction}
 * only to drive that outcome.
 *
 * <p>See {@link DistributedTransactionBackedGlobalTransactionManager} for how the manager is wired
 * and the global transaction and its branches are begun.
 */
@NotThreadSafe
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
