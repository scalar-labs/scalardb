package com.scalar.db.api;

import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import java.util.Optional;

public interface DistributedTransactionManager
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
   * Begins a new transaction.
   *
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction fails to begin due to transient faults.
   *     You can retry the transaction
   * @throws TransactionException if the transaction fails to begin due to transient or nontransient
   *     faults. You can try retrying the transaction, but you may not be able to begin the
   *     transaction due to nontransient faults
   */
  DistributedTransaction begin() throws TransactionNotFoundException, TransactionException;

  /**
   * Begins a new transaction with the specified transaction ID. It is users' responsibility to
   * guarantee uniqueness of the ID, so it is not recommended to use this method unless you know
   * exactly what you are doing.
   *
   * @param txId an user-provided unique transaction ID
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction fails to begin due to transient faults.
   *     You can retry the transaction
   * @throws TransactionException if the transaction fails to begin due to transient or nontransient
   *     faults. You can try retrying the transaction, but you may not be able to begin the
   *     transaction due to nontransient faults
   */
  DistributedTransaction begin(String txId)
      throws TransactionNotFoundException, TransactionException;

  /**
   * Begins a new transaction in read-only mode.
   *
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction fails to begin due to transient faults.
   *     You can retry the transaction
   * @throws TransactionException if the transaction fails to begin due to transient or nontransient
   *     faults. You can try retrying the transaction, but you may not be able to begin the
   *     transaction due to nontransient faults
   */
  DistributedTransaction beginReadOnly() throws TransactionNotFoundException, TransactionException;

  /**
   * Begins a new transaction with the specified transaction ID in read-only mode. It is users'
   * responsibility to guarantee uniqueness of the ID, so it is not recommended to use this method
   * unless you know exactly what you are doing.
   *
   * @param txId an user-provided unique transaction ID
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction fails to begin due to transient faults.
   *     You can retry the transaction
   * @throws TransactionException if the transaction fails to begin due to transient or nontransient
   *     faults. You can try retrying the transaction, but you may not be able to begin the
   *     transaction due to nontransient faults
   */
  DistributedTransaction beginReadOnly(String txId)
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
  default DistributedTransaction start() throws TransactionNotFoundException, TransactionException {
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
  default DistributedTransaction start(String txId)
      throws TransactionNotFoundException, TransactionException {
    return begin(txId);
  }

  /**
   * Starts a new transaction in read-only mode. This method is an alias of {@link
   * #beginReadOnly()}.
   *
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction fails to start due to transient faults.
   *     You can retry the transaction
   * @throws TransactionException if the transaction fails to start due to transient or nontransient
   *     faults. You can try retrying the transaction, but you may not be able to start the
   *     transaction due to nontransient faults
   */
  default DistributedTransaction startReadOnly()
      throws TransactionNotFoundException, TransactionException {
    return beginReadOnly();
  }

  /**
   * Starts a new transaction with the specified transaction ID in read-only mode. This method is an
   * alias of {@link #beginReadOnly(String)}.
   *
   * @param txId an user-provided unique transaction ID
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction fails to start due to transient faults.
   *     You can retry the transaction
   * @throws TransactionException if the transaction fails to start due to transient or nontransient
   *     faults. You can try retrying the transaction, but you may not be able to start the
   *     transaction due to nontransient faults
   */
  default DistributedTransaction startReadOnly(String txId)
      throws TransactionNotFoundException, TransactionException {
    return beginReadOnly(txId);
  }

  /**
   * Starts a new transaction with the specified {@link Isolation} level.
   *
   * @param isolation an isolation level
   * @return {@link DistributedTransaction}
   * @throws TransactionException if starting the transaction fails
   * @deprecated As of release 2.4.0. Will be removed in release 4.0.0.
   */
  @Deprecated
  DistributedTransaction start(Isolation isolation) throws TransactionException;

  /**
   * Starts a new transaction with the specified transaction ID and {@link Isolation} level. It is
   * users' responsibility to guarantee uniqueness of the ID, so it is not recommended to use this
   * method unless you know exactly what you are doing.
   *
   * @param txId an user-provided unique transaction ID
   * @param isolation an isolation level
   * @return {@link DistributedTransaction}
   * @throws TransactionException if starting the transaction fails
   * @deprecated As of release 2.4.0. Will be removed in release 4.0.0.
   */
  @Deprecated
  DistributedTransaction start(String txId, Isolation isolation) throws TransactionException;

  /**
   * Starts a new transaction with the specified {@link Isolation} level and {@link
   * SerializableStrategy}. If the isolation is not SERIALIZABLE, the serializable strategy is
   * ignored.
   *
   * @param isolation an isolation level
   * @param strategy a serializable strategy
   * @return {@link DistributedTransaction}
   * @throws TransactionException if starting the transaction fails
   * @deprecated As of release 2.4.0. Will be removed in release 4.0.0.
   */
  @Deprecated
  DistributedTransaction start(Isolation isolation, SerializableStrategy strategy)
      throws TransactionException;

  /**
   * Starts a new transaction with Serializable isolation level and the specified {@link
   * SerializableStrategy}.
   *
   * @param strategy a serializable strategy
   * @return {@link DistributedTransaction}
   * @throws TransactionException if starting the transaction fails
   * @deprecated As of release 2.4.0. Will be removed in release 4.0.0.
   */
  @Deprecated
  DistributedTransaction start(SerializableStrategy strategy) throws TransactionException;

  /**
   * Starts a new transaction with the specified transaction ID, Serializable isolation level and
   * the specified {@link SerializableStrategy}. It is users' responsibility to guarantee uniqueness
   * of the ID, so it is not recommended to use this method unless you know exactly what you are
   * doing.
   *
   * @param txId an user-provided unique transaction ID
   * @param strategy a serializable strategy
   * @return {@link DistributedTransaction}
   * @throws TransactionException if starting the transaction fails
   * @deprecated As of release 2.4.0. Will be removed in release 4.0.0.
   */
  @Deprecated
  DistributedTransaction start(String txId, SerializableStrategy strategy)
      throws TransactionException;

  /**
   * Starts a new transaction with the specified transaction ID, {@link Isolation} level and {@link
   * SerializableStrategy}. It is users' responsibility to guarantee uniqueness of the ID, so it is
   * not recommended to use this method unless you know exactly what you are doing. If the isolation
   * is not SERIALIZABLE, the serializable strategy is ignored.
   *
   * @param txId an user-provided unique transaction ID
   * @param isolation an isolation level
   * @param strategy a serializable strategy
   * @return {@link DistributedTransaction}
   * @throws TransactionException if starting the transaction fails
   * @deprecated As of release 2.4.0. Will be removed in release 4.0.0.
   */
  @Deprecated
  DistributedTransaction start(String txId, Isolation isolation, SerializableStrategy strategy)
      throws TransactionException;

  /**
   * Joins an ongoing transaction associated with the specified transaction ID.
   *
   * @param txId the transaction ID
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction associated with the specified
   *     transaction ID is not found. You can retry the transaction from the beginning
   */
  default DistributedTransaction join(String txId) throws TransactionNotFoundException {
    return resume(txId);
  }

  /**
   * Resumes an ongoing transaction associated with the specified transaction ID.
   *
   * @param txId the transaction ID
   * @return {@link DistributedTransaction}
   * @throws TransactionNotFoundException if the transaction associated with the specified
   *     transaction ID is not found. You can retry the transaction from the beginning
   */
  DistributedTransaction resume(String txId) throws TransactionNotFoundException;

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
