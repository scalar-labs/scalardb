package com.scalar.db.dataloader.core.dataimport.dao;

import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.service.TransactionFactory;

/**
 * A manager class for handling ScalarDB operations in transaction mode.
 *
 * <p>Provides access to {@link DistributedTransactionManager} for data operations
 *
 * <p>This class is typically used when interacting with ScalarDB in a transactional configuration.
 */
public class ScalarDbTransactionManager {

  private final DistributedTransactionManager transactionManager;

  /**
   * Constructs a {@code ScalarDbTransactionManager} using the provided {@link TransactionFactory}.
   *
   * @param transactionFactory the factory used to create the ScalarDB storage and admin instances
   */
  public ScalarDbTransactionManager(TransactionFactory transactionFactory) {
    transactionManager = transactionFactory.getTransactionManager();
  }

  /**
   * Returns distributed Transaction manager for ScalarDB connection that is running in transaction
   * mode
   *
   * @return distributed transaction manager object
   */
  public DistributedTransactionManager getDistributedTransactionManager() {
    return transactionManager;
  }
}
