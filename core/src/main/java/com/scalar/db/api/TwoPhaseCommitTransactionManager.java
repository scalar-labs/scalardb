package com.scalar.db.api;

import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import java.util.Optional;

/**
 * A transaction manager based on a two-phase commit protocol.
 *
 * <p>Just like a well-known two-phase commit protocol, there are two roles, a coordinator and a
 * participant, that execute a single transaction collaboratively. A coordinator process first
 * starts a transaction with {@link #start()} or {@link #start(String)}, and participant processes
 * join the transaction with {@link #join(String)} with the transaction ID. Also, participants can
 * resume the transaction with {@link #resume(String)} with the transaction ID.
 */
public interface TwoPhaseCommitTransactionManager
    extends TransactionManagerCrudOperable, AutoCloseable {

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
   * Begins a new transaction. This method is assumed to be called by a coordinator process.
   *
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction fails to begin due to transient faults.
   *     You can retry the transaction
   * @throws TransactionException if the transaction fails to begin due to transient or nontransient
   *     faults. You can try retrying the transaction, but you may not be able to begin the
   *     transaction due to nontransient faults
   */
  TwoPhaseCommitTransaction begin() throws TransactionNotFoundException, TransactionException;

  /**
   * Begins a new transaction with the specified transaction ID. It is users' responsibility to
   * guarantee uniqueness of the ID, so it is not recommended to use this method unless you know
   * exactly what you are doing. This method is assumed to be called by a coordinator process.
   *
   * @param txId an user-provided unique transaction ID
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction fails to begin due to transient faults.
   *     You can retry the transaction
   * @throws TransactionException if the transaction fails to begin due to transient or nontransient
   *     faults. You can try retrying the transaction, but you may not be able to begin the
   *     transaction due to nontransient faults
   */
  TwoPhaseCommitTransaction begin(String txId)
      throws TransactionNotFoundException, TransactionException;

  /**
   * Starts a new transaction. This method is an alias of {@link #begin()}.
   *
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction fails to start due to transient faults.
   *     You can retry the transaction
   * @throws TransactionException if the transaction fails to start due to transient or nontransient
   *     faults. You can try retrying the transaction, but you may not be able to start the
   *     transaction due to nontransient faults
   */
  default TwoPhaseCommitTransaction start()
      throws TransactionNotFoundException, TransactionException {
    return begin();
  }

  /**
   * Starts a new transaction with the specified transaction ID. This method is an alias of {@link
   * #begin(String)}.
   *
   * @param txId an user-provided unique transaction ID
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction fails to start due to transient faults.
   *     You can retry the transaction
   * @throws TransactionException if the transaction fails to start due to transient or nontransient
   *     faults. You can try retrying the transaction, but you may not be able to start the
   *     transaction due to nontransient faults
   */
  default TwoPhaseCommitTransaction start(String txId)
      throws TransactionNotFoundException, TransactionException {
    return begin(txId);
  }

  /**
   * Joins a transaction associated with the specified transaction ID. The transaction should be
   * started by a coordinator process. This method is assumed to be called by a participant process.
   *
   * @param txId the transaction ID
   * @return {@link TwoPhaseCommitTransaction}
   * @throws TransactionNotFoundException if joining the transaction fails due to transient faults.
   *     You can retry the transaction from the beginning
   * @throws TransactionException if joining the transaction fails due to transient or nontransient
   *     faults. You can try retrying the transaction from the beginning, but the transaction may
   *     still fail if the cause is nontranient
   */
  TwoPhaseCommitTransaction join(String txId)
      throws TransactionNotFoundException, TransactionException;

  /**
   * Resumes an ongoing transaction associated with the specified transaction ID.
   *
   * @param txId the transaction ID
   * @return {@link TwoPhaseCommitTransaction}
   * @throws TransactionNotFoundException if the transaction associated with the specified
   *     transaction ID is not found. You can retry the transaction from the beginning
   */
  TwoPhaseCommitTransaction resume(String txId) throws TransactionNotFoundException;

  /**
   * Returns the state of a given transaction.
   *
   * @param txId a transaction ID
   * @return {@link TransactionState}
   * @throws TransactionException if getting the state of a given transaction fails
   */
  TransactionState getState(String txId) throws TransactionException;

  /**
   * Rolls back a given transaction.
   *
   * @param txId a transaction ID
   * @return {@link TransactionState}
   * @throws TransactionException if rolling back the given transaction fails
   */
  TransactionState rollback(String txId) throws TransactionException;

  /**
   * Aborts a given transaction. This method is an alias of {@link #rollback(String)}.
   *
   * @param txId a transaction ID
   * @return {@link TransactionState}
   * @throws TransactionException if aborting the given transaction fails
   */
  default TransactionState abort(String txId) throws TransactionException {
    return rollback(txId);
  }

  /**
   * Closes connections to the cluster. The connections are shared among multiple services such as
   * StorageService and TransactionService, thus this should only be used when closing applications.
   */
  @Override
  void close();
}
