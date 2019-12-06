package com.scalar.database.api;

public interface DistributedTransactionManager {

  void with(String namespace, String tableName);

  /**
   * Starts a new transaction.
   *
   * @return {@link DistributedTransaction}
   */
  DistributedTransaction start();

  /**
   * Starts a new transaction with the specified transaction ID. It is users' responsibility to
   * guarantee uniqueness of the ID so it is not recommended to use this method unless you know
   * exactly what you are doing.
   *
   * @param txId an user-provided unique transaction ID
   * @return {@link DistributedTransaction}
   */
  DistributedTransaction start(String txId);

  /**
   * Starts a new transaction with the specified {@link Isolation} level.
   *
   * @param isolation an isolation level
   * @return {@link DistributedTransaction}
   */
  DistributedTransaction start(Isolation isolation);

  /**
   * Starts a new transaction with the specified transaction ID and {@link Isolation} level. It is
   * users' responsibility to guarantee uniqueness of the ID so it is not recommended to use this
   * method unless you know exactly what you are doing.
   *
   * @param txId an user-provided unique transaction ID
   * @param isolation an isolation level
   * @return {@link DistributedTransaction}
   */
  DistributedTransaction start(String txId, Isolation isolation);

  /**
   * Returns the state of a given transaction.
   *
   * @param txId a transaction ID
   * @return {@link TransactionState}
   */
  TransactionState getState(String txId);

  /**
   * Closes connections to the cluster. The connections are shared among multiple services such as
   * StorageService and TransactionService, thus this should only be used when closing applications.
   */
  void close();
}
