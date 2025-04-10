package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.DistributedTransactionProvider;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.common.ActiveTransactionManagedDistributedTransactionManager;
import com.scalar.db.common.ActiveTransactionManagedTwoPhaseCommitTransactionManager;
import com.scalar.db.common.StateManagedDistributedTransactionManager;
import com.scalar.db.common.StateManagedTwoPhaseCommitTransactionManager;
import com.scalar.db.config.DatabaseConfig;

public class ConsensusCommitProvider implements DistributedTransactionProvider {
  @Override
  public String getName() {
    return ConsensusCommitConfig.TRANSACTION_MANAGER_NAME;
  }

  @Override
  public DistributedTransactionManager createDistributedTransactionManager(DatabaseConfig config) {
    return new ActiveTransactionManagedDistributedTransactionManager(
        new StateManagedDistributedTransactionManager(new ConsensusCommitManager(config)),
        config.getActiveTransactionManagementExpirationTimeMillis());
  }

  @Override
  public DistributedTransactionAdmin createDistributedTransactionAdmin(DatabaseConfig config) {
    return new ConsensusCommitAdmin(config);
  }

  @Override
  public TwoPhaseCommitTransactionManager createTwoPhaseCommitTransactionManager(
      DatabaseConfig config) {
    return new ActiveTransactionManagedTwoPhaseCommitTransactionManager(
        new StateManagedTwoPhaseCommitTransactionManager(
            new TwoPhaseConsensusCommitManager(config)),
        config.getActiveTransactionManagementExpirationTimeMillis());
  }
}
