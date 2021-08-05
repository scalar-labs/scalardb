package com.scalar.db.api;

import com.scalar.db.exception.transaction.TransactionException;
import java.util.Optional;

public interface DistributedTransactionManager {

  void with(String namespace, String tableName);

  void withNamespace(String namespace);

  Optional<String> getNamespace();

  void withTable(String tableName);

  Optional<String> getTable();

  /**
   * Starts a new transaction.
   *
   * @return {@link DistributedTransaction}
   * @throws TransactionException if starting the transaction failed
   */
  DistributedTransaction start() throws TransactionException;

  /**
   * Starts a new transaction with the specified transaction ID. It is users' responsibility to
   * guarantee uniqueness of the ID so it is not recommended to use this method unless you know
   * exactly what you are doing.
   *
   * @param txId an user-provided unique transaction ID
   * @return {@link DistributedTransaction}
   * @throws TransactionException if starting the transaction failed
   */
  DistributedTransaction start(String txId) throws TransactionException;

  /**
   * Starts a new transaction with the specified {@link Isolation} level.
   *
   * @param isolation an isolation level
   * @return {@link DistributedTransaction}
   * @throws TransactionException if starting the transaction failed
   * @deprecated As of release 2.4.0. Will be removed in release 4.0.0.
   */
  @Deprecated
  DistributedTransaction start(Isolation isolation) throws TransactionException;

  /**
   * Starts a new transaction with the specified transaction ID and {@link Isolation} level. It is
   * users' responsibility to guarantee uniqueness of the ID so it is not recommended to use this
   * method unless you know exactly what you are doing.
   *
   * @param txId an user-provided unique transaction ID
   * @param isolation an isolation level
   * @return {@link DistributedTransaction}
   * @throws TransactionException if starting the transaction failed
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
   * @throws TransactionException if starting the transaction failed
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
   * @throws TransactionException if starting the transaction failed
   * @deprecated As of release 2.4.0. Will be removed in release 4.0.0.
   */
  @Deprecated
  DistributedTransaction start(SerializableStrategy strategy) throws TransactionException;

  /**
   * Starts a new transaction with the specified transaction ID, Serializable isolation level and
   * the specified {@link SerializableStrategy}. It is users' responsibility to guarantee uniqueness
   * of the ID so it is not recommended to use this method unless you know exactly what you are
   * doing.
   *
   * @param txId an user-provided unique transaction ID
   * @param strategy a serializable strategy
   * @return {@link DistributedTransaction}
   * @throws TransactionException if starting the transaction failed
   * @deprecated As of release 2.4.0. Will be removed in release 4.0.0.
   */
  @Deprecated
  DistributedTransaction start(String txId, SerializableStrategy strategy)
      throws TransactionException;

  /**
   * Starts a new transaction with the specified transaction ID, {@link Isolation} level and {@link
   * SerializableStrategy}. It is users' responsibility to guarantee uniqueness of the ID so it is
   * not recommended to use this method unless you know exactly what you are doing. If the isolation
   * is not SERIALIZABLE, the serializable strategy is ignored.
   *
   * @param txId an user-provided unique transaction ID
   * @param isolation an isolation level
   * @param strategy a serializable strategy
   * @return {@link DistributedTransaction}
   * @throws TransactionException if starting the transaction failed
   * @deprecated As of release 2.4.0. Will be removed in release 4.0.0.
   */
  @Deprecated
  DistributedTransaction start(String txId, Isolation isolation, SerializableStrategy strategy)
      throws TransactionException;

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
