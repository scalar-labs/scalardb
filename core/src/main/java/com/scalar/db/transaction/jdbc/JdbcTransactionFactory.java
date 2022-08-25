package com.scalar.db.transaction.jdbc;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionFactory;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.config.DatabaseConfig;

public class JdbcTransactionFactory implements DistributedTransactionFactory {
  @Override
  public String getName() {
    return "jdbc";
  }

  @Override
  public DistributedTransactionManager getDistributedTransactionManager(DatabaseConfig config) {
    return new JdbcTransactionManager(config);
  }

  @Override
  public DistributedTransactionAdmin getDistributedTransactionAdmin(DatabaseConfig config) {
    return new JdbcTransactionAdmin(config);
  }

  @Override
  public TwoPhaseCommitTransactionManager getTwoPhaseCommitTransactionManager(
      DatabaseConfig config) {
    return null;
  }
}
