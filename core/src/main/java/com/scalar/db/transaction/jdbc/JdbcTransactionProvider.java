package com.scalar.db.transaction.jdbc;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.DistributedTransactionProvider;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.common.ActiveTransactionManagedDistributedTransactionManager;
import com.scalar.db.common.StateManagedDistributedTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcConfig;
import javax.annotation.Nullable;

public class JdbcTransactionProvider implements DistributedTransactionProvider {
  @Override
  public String getName() {
    return JdbcConfig.TRANSACTION_MANAGER_NAME;
  }

  @Override
  public DistributedTransactionManager createDistributedTransactionManager(DatabaseConfig config) {
    return new ActiveTransactionManagedDistributedTransactionManager(
        new StateManagedDistributedTransactionManager(new JdbcTransactionManager(config)),
        config.getActiveTransactionManagementExpirationTimeMillis());
  }

  @Override
  public DistributedTransactionAdmin createDistributedTransactionAdmin(DatabaseConfig config) {
    return new JdbcTransactionAdmin(config);
  }

  @Nullable
  @Override
  public TwoPhaseCommitTransactionManager createTwoPhaseCommitTransactionManager(
      DatabaseConfig config) {
    return null;
  }
}
