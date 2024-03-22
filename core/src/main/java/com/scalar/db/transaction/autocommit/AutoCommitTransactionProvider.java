package com.scalar.db.transaction.autocommit;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.DistributedTransactionProvider;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import javax.annotation.Nullable;

public class AutoCommitTransactionProvider implements DistributedTransactionProvider {

  @Override
  public String getName() {
    return AutoCommitTransactionConfig.TRANSACTION_MANAGER_NAME;
  }

  @Override
  public DistributedTransactionManager createDistributedTransactionManager(DatabaseConfig config) {
    return new AutoCommitTransactionManager(config);
  }

  @Override
  public DistributedTransactionAdmin createDistributedTransactionAdmin(DatabaseConfig config) {
    return new AutoCommitTransactionAdmin(config);
  }

  @Nullable
  @Override
  public TwoPhaseCommitTransactionManager createTwoPhaseCommitTransactionManager(
      DatabaseConfig config) {
    return null;
  }
}
