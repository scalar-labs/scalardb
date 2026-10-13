package com.scalar.db.common;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.exception.transaction.TransactionNotFoundException;

/**
 * A {@link DistributedTransactionManager} that can return an ongoing transaction it began, by the
 * transaction ID. It is for a server that continues a transaction across requests, where each
 * request carries only the transaction ID.
 */
public interface ResumableDistributedTransactionManager extends DistributedTransactionManager {

  /**
   * Returns the ongoing transaction that this manager began with the specified transaction ID.
   * Depending on the implementation, an unknown transaction ID either makes this method throw
   * {@link TransactionNotFoundException} or makes the operations on the returned transaction fail.
   *
   * @param txId the transaction ID
   * @return the ongoing transaction associated with the transaction ID
   * @throws TransactionNotFoundException if the implementation resolves the transaction ID eagerly
   *     and no ongoing transaction is associated with it
   */
  DistributedTransaction resume(String txId) throws TransactionNotFoundException;
}
