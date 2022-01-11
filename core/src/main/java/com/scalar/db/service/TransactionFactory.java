package com.scalar.db.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.config.DatabaseConfig;

/**
 * A factory class to instantiate {@link DistributedTransactionManager} and {@link
 * TwoPhaseCommitTransactionManager}
 */
public class TransactionFactory {
  private final Injector injector;

  public TransactionFactory(DatabaseConfig config) {
    injector = Guice.createInjector(new TransactionModule(config));
  }

  /**
   * Returns a {@link DistributedTransactionManager} instance
   *
   * @return a {@link DistributedTransactionManager} instance
   */
  public DistributedTransactionManager getTransactionManager() {
    return injector.getInstance(DistributedTransactionManager.class);
  }

  /**
   * Returns a {@link TwoPhaseCommitTransactionManager} instance
   *
   * @return a {@link TwoPhaseCommitTransactionManager} instance
   */
  public TwoPhaseCommitTransactionManager getTwoPhaseCommitTransactionManager() {
    return injector.getInstance(TwoPhaseCommitTransactionManager.class);
  }
}
