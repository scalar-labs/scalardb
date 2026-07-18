package com.scalar.db.common;

import com.scalar.db.api.GlobalTransaction;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A {@link GlobalTransaction} that forwards each abstract method to a wrapped global transaction.
 *
 * <p>Base class for global-transaction decorators: subclasses override only the methods they need
 * and inherit plain delegation for the rest. {@link GlobalTransaction#abort()} is deliberately not
 * delegated: the interface supplies it as a default implementation defined in terms of {@link
 * GlobalTransaction#rollback()}, which this class already forwards.
 */
public abstract class DecoratedGlobalTransaction implements GlobalTransaction {

  private final GlobalTransaction globalTransaction;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  protected DecoratedGlobalTransaction(GlobalTransaction globalTransaction) {
    this.globalTransaction = globalTransaction;
  }

  @Override
  public String getId() {
    return globalTransaction.getId();
  }

  @Override
  public void commit() throws CommitException, UnknownTransactionStatusException {
    globalTransaction.commit();
  }

  @Override
  public void rollback() throws RollbackException {
    globalTransaction.rollback();
  }
}
