package com.scalar.db.api;

import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;

/**
 * The handle for one branch of a global transaction, obtained via {@link
 * GlobalTransactionManager#beginBranch(String)}.
 *
 * <p>A branch performs CRUD (inherited from {@link TransactionCrudOperable}) against its own
 * portion of the data. The transaction's overall outcome — commit or rollback — is driven by the
 * owning {@link GlobalTransaction}, not by the branch.
 */
public interface BranchTransaction extends TransactionCrudOperable {

  /**
   * Returns the ID of the global transaction this branch belongs to.
   *
   * @return the global transaction ID
   */
  String getId();

  /**
   * Ends the branch. This does not commit the transaction; the outcome is still driven by the
   * owning global transaction.
   *
   * <p>Every branch must call this exactly once, after its last CRUD operation and before the
   * owning {@link GlobalTransaction} is committed or rolled back. Issuing CRUD on this branch once
   * {@code end()} has been called is not allowed.
   *
   * @throws CrudConflictException if ending the branch fails due to transient faults (e.g., a
   *     conflict). You can retry the transaction from the beginning
   * @throws CrudException if ending the branch fails due to transient or nontransient faults
   */
  void end() throws CrudConflictException, CrudException;
}
