package com.scalar.db.api;

import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;

/**
 * The overall handle of a global transaction begun via {@link
 * GlobalTransactionManager#beginGlobal()}.
 *
 * <p>It drives the transaction's outcome across all its branches — {@link #commit()} or {@link
 * #rollback()} — but does not itself perform CRUD. CRUD is issued on the {@link BranchTransaction}
 * handles that join this transaction by its {@link #getId() ID}.
 */
public interface GlobalTransaction {

  /**
   * Returns the ID of this global transaction. Pass it to {@link
   * GlobalTransactionManager#beginBranch(String)} on each branch to join this transaction.
   *
   * @return the global transaction ID
   */
  String getId();

  /**
   * Commits this global transaction, making the writes of all its branches durable as a whole.
   *
   * @throws CommitConflictException if the commit fails due to transient faults (e.g., a conflict).
   *     You can retry the transaction from the beginning
   * @throws CommitException if the commit fails due to transient or nontransient faults
   * @throws UnknownTransactionStatusException if the commit status cannot be determined. The
   *     outcome is indeterminate — the transaction may or may not have been committed. Do not
   *     blindly retry or roll back; determine the outcome before deciding how to proceed
   */
  void commit() throws CommitConflictException, CommitException, UnknownTransactionStatusException;

  /**
   * Rolls back this global transaction, discarding the writes of all its branches.
   *
   * @throws RollbackException if the rollback fails due to transient or nontransient faults
   */
  void rollback() throws RollbackException;

  /**
   * Aborts a transaction. This method is an alias of {@link #rollback()}.
   *
   * @throws AbortException if the transaction fails to abort due to transient or nontransient
   *     faults
   */
  default void abort() throws AbortException {
    try {
      rollback();
    } catch (RollbackException e) {
      throw new AbortException(e.getMessage(), e, e.getTransactionId().orElse(null));
    }
  }
}
