package com.scalar.db.dataloader.core.dataimport.dao;

import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.service.TransactionFactory;
import java.io.IOException;

public class ScalarDbTransactionManger {

  private final DistributedTransactionManager transactionManager;

  public ScalarDbTransactionManger(TransactionFactory transactionFactory) throws IOException {
    transactionManager = transactionFactory.getTransactionManager();
  }

  /**
   * @return Distributed Transaction manager for ScalarDB connection that is running in transaction
   *     mode
   */
  public DistributedTransactionManager getDistributedTransactionManager() {
    return transactionManager;
  }
}
