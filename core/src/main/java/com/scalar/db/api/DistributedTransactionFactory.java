package com.scalar.db.api;

import com.scalar.db.config.DatabaseConfig;

/**
 * A factory class that creates {@link DistributedTransactionManager}, {@link
 * DistributedTransactionAdmin}, and {@link TwoPhaseCommitTransactionManager} instances. Each
 * transaction manager should implement this class to instantiate its implementations of {@link
 * DistributedTransactionManager}, {@link DistributedTransactionAdmin}, and {@link
 * TwoPhaseCommitTransactionManager}. The implementations are assumed to be loaded by {@link
 * java.util.ServiceLoader}.
 */
public interface DistributedTransactionFactory {

  /**
   * Returns the name of the adapter. This is for the configuration "scalar.db.transaction_manager".
   *
   * @return the name of the adapter
   */
  String getName();

  /**
   * Creates an instance of {@link DistributedTransactionManager} for the transaction manager.
   *
   * @param config a database config
   * @return an instance of {@link DistributedTransactionManager} for the transaction manager
   */
  DistributedTransactionManager getDistributedTransactionManager(DatabaseConfig config);

  /**
   * Creates an instance of {@link DistributedTransactionAdmin} for the transaction manager.
   *
   * @param config a database config
   * @return an instance of {@link DistributedTransactionAdmin} for the transaction manager
   */
  DistributedTransactionAdmin getDistributedTransactionAdmin(DatabaseConfig config);

  /**
   * Creates an instance of {@link TwoPhaseCommitTransactionManager} for the transaction manager.
   *
   * @param config a database config
   * @return an instance of {@link TwoPhaseCommitTransactionManager} for the transaction manager
   */
  TwoPhaseCommitTransactionManager getTwoPhaseCommitTransactionManager(DatabaseConfig config);
}
