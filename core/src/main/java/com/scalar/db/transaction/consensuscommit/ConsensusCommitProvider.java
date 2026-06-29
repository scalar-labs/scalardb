package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TwoPhaseCommit;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.common.AbstractDistributedTransactionProvider;
import com.scalar.db.config.DatabaseConfig;

public class ConsensusCommitProvider extends AbstractDistributedTransactionProvider {

  @Override
  public String getName() {
    return ConsensusCommitConfig.TRANSACTION_MANAGER_NAME;
  }

  @Override
  public DistributedTransactionManager createRawDistributedTransactionManager(
      DatabaseConfig config) {
    return new ConsensusCommitManager(config);
  }

  @Override
  public DistributedTransactionAdmin createDistributedTransactionAdmin(DatabaseConfig config) {
    return new ConsensusCommitAdmin(config);
  }

  @Override
  public TwoPhaseCommitTransactionManager createRawTwoPhaseCommitTransactionManager(
      DatabaseConfig config) {
    return new TwoPhaseConsensusCommitManager(config);
  }

  @Override
  public TwoPhaseCommit.Coordinator createRawTwoPhaseCommitCoordinator(DatabaseConfig config) {
    return new ConsensusCommitCoordinator(config);
  }

  @Override
  public TwoPhaseCommit.Participant createRawTwoPhaseCommitParticipant(DatabaseConfig config) {
    return new ConsensusCommitParticipant(config);
  }
}
