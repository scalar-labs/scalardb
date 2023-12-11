package com.scalar.db.transaction.jdbc;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.DistributedTransactionProvider;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcConfig;

public class JdbcTransactionProvider implements DistributedTransactionProvider {
  @Override
  public String getName() {
    return JdbcConfig.TRANSACTION_MANAGER_NAME;
  }

  @Override
  public DistributedTransactionManager createDistributedTransactionManager(DatabaseConfig config) {
    return new JdbcTransactionManager(config);
  }

  @Override
  public DistributedTransactionAdmin createDistributedTransactionAdmin(DatabaseConfig config) {
    return new JdbcTransactionAdmin(config);
  }

  @Override
  public TwoPhaseCommitTransactionManager createTwoPhaseCommitTransactionManager(
      DatabaseConfig config) {
    return null;
  }
}
