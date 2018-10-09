package com.scalar.database.api;

public interface DistributedTransactionManager {

  void with(String namespace, String tableName);

  DistributedTransaction start();

  DistributedTransaction start(Isolation isolation);

  /**
   * Closes connections to the cluster. The connections are shared among multiple services such as
   * StorageService and TransactionService, thus this should only be used when closing applications.
   */
  void close();
}
