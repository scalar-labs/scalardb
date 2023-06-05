package com.scalar.db.api;

import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.PreparationUnsatisfiedConditionException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.exception.transaction.ValidationException;
import java.util.Optional;

/**
 * A transaction abstraction based on a two-phase commit protocol for interacting with the
 * underlying storage and database implementations.
 */
public interface TwoPhaseCommitTransaction extends TransactionCrudOperable {

  /**
   * Returns the ID of a transaction.
   *
   * @return the ID of a transaction
   */
  String getId();

  /**
   * Sets the specified namespace and the table name as default values in the instance.
   *
   * @param namespace default namespace to operate for
   * @param tableName default table name to operate for
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  void with(String namespace, String tableName);

  /**
   * Sets the specified namespace as a default value in the instance.
   *
   * @param namespace default namespace to operate for
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  void withNamespace(String namespace);

  /**
   * Returns the namespace.
   *
   * @return an {@code Optional} with the namespace
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  Optional<String> getNamespace();

  /**
   * Sets the specified table name as a default value in the instance.
   *
   * @param tableName default table name to operate for
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  void withTable(String tableName);

  /**
   * Returns the table name.
   *
   * @return an {@code Optional} with the table name
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  Optional<String> getTable();

  /**
   * Prepares a transaction.
   *
   * @throws PreparationConflictException if a transaction conflict occurs. You can retry the
   *     transaction from the beginning in this case
   * @throws PreparationException if the operation fails
   * @throws PreparationUnsatisfiedConditionException if the condition set on a mutation operation
   *     is not satisfied
   */
  void prepare()
      throws PreparationConflictException, PreparationException,
          PreparationUnsatisfiedConditionException;

  /**
   * Validates a transaction. Depending on the concurrency control algorithm, you need a validation
   * phase for a transaction.
   *
   * @throws ValidationConflictException if a transaction conflict occurs. You can retry the
   *     transaction from the beginning in this case
   * @throws ValidationException if the operation fails
   */
  void validate() throws ValidationConflictException, ValidationException;

  /**
   * Commits a transaction.
   *
   * @throws CommitConflictException if a transaction conflict occurs. You can retry the transaction
   *     from the beginning in this case
   * @throws CommitException if the operation fails
   * @throws UnknownTransactionStatusException if the status of the commit is unknown
   */
  void commit() throws CommitConflictException, CommitException, UnknownTransactionStatusException;

  /**
   * Rolls back a transaction.
   *
   * @throws RollbackException if the operation fails
   */
  void rollback() throws RollbackException;

  /**
   * Aborts a transaction. This method is an alias of {@link #rollback()}.
   *
   * @throws AbortException if the operation fails
   */
  default void abort() throws AbortException {
    try {
      rollback();
    } catch (RollbackException e) {
      throw new AbortException(e.getMessage(), e, e.getTransactionId().orElse(null));
    }
  }
}
