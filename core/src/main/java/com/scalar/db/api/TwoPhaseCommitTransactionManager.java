package com.scalar.db.api;

import com.scalar.db.exception.transaction.TransactionException;
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
public interface TwoPhaseCommitTransactionManager {

  /**
   * Sets the specified namespace and the table name as default values in the instance
   *
   * @param namespace default namespace to operate for
   * @param tableName default table name to operate for
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  void with(String namespace, String tableName);

  /**
   * Sets the specified namespace as a default value in the instance
   *
   * @param namespace default namespace to operate for
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  void withNamespace(String namespace);

  /**
   * Returns the namespace
   *
   * @return an {@code Optional} with the namespace
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  Optional<String> getNamespace();

  /**
   * Sets the specified table name as a default value in the instance
   *
   * @param tableName default table name to operate for
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  void withTable(String tableName);

  /**
   * Returns the table name
   *
   * @return an {@code Optional} with the table name
   * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
   */
  @Deprecated
  Optional<String> getTable();

  /**
   * Starts a new transaction. This method is assumed to be called by a coordinator process.
   *
   * @return {@link TwoPhaseCommitTransaction}
   * @throws TransactionException if starting the transaction failed
   */
  TwoPhaseCommitTransaction start() throws TransactionException;

  /**
   * Starts a new transaction with the specified transaction ID. It is users' responsibility to
   * guarantee uniqueness of the ID so it is not recommended to use this method unless you know
   * exactly what you are doing. This method is assumed to be called by a coordinator process.
   *
   * @param txId an user-provided unique transaction ID
   * @return {@link TwoPhaseCommitTransaction}
   * @throws TransactionException if starting the transaction failed
   */
  TwoPhaseCommitTransaction start(String txId) throws TransactionException;

  /**
   * Joins a transaction associated with the specified transaction ID. The transaction should be
   * started by a coordinator process. This method is assumed to be called by a participant process.
   *
   * @param txId the transaction ID
   * @return {@link TwoPhaseCommitTransaction}
   * @throws TransactionException if participating the transaction failed
   */
  TwoPhaseCommitTransaction join(String txId) throws TransactionException;

  /**
   * Suspends a transaction. You can resume this transaction with {@link #resume(String)}.
   *
   * @param transaction a transaction to suspend
   * @throws TransactionException if suspending the transaction failed
   */
  void suspend(TwoPhaseCommitTransaction transaction) throws TransactionException;

  /**
   * Resumes a suspended transaction associated with the specified transaction ID.
   *
   * @param txId the transaction ID
   * @return {@link TwoPhaseCommitTransaction}
   * @throws TransactionException if resuming the transaction failed
   */
  TwoPhaseCommitTransaction resume(String txId) throws TransactionException;

  /**
   * Returns the state of a given transaction.
   *
   * @param txId a transaction ID
   * @return {@link TransactionState}
   * @throws TransactionException if getting the state of a given transaction failed
   */
  TransactionState getState(String txId) throws TransactionException;

  /**
   * Aborts a given transaction.
   *
   * @param txId a transaction ID
   * @return {@link TransactionState}
   * @throws TransactionException if aborting the given transaction failed
   */
  TransactionState abort(String txId) throws TransactionException;

  /**
   * Closes connections to the cluster. The connections are shared among multiple services such as
   * StorageService and TransactionService, thus this should only be used when closing applications.
   */
  void close();
}
