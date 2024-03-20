package com.scalar.db.transaction.autocommit;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.DistributedTransactionProvider;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.config.DatabaseConfig;

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

  @Override
  public TwoPhaseCommitTransactionManager createTwoPhaseCommitTransactionManager(
      DatabaseConfig config) {
    throw new UnsupportedOperationException(
        CoreError.AUTO_COMMIT_TRANSACTION_TWO_PHASE_COMMIT_NOT_SUPPORTED.buildMessage());
  }
}
