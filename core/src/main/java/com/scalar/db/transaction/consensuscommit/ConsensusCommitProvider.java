package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.GlobalTransactionManager;
import com.scalar.db.api.TwoPhaseCommitCoordinator;
import com.scalar.db.api.TwoPhaseCommitParticipant;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.common.AbstractDistributedTransactionProvider;
import com.scalar.db.common.CoreError;
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
  public TwoPhaseCommitCoordinator createRawTwoPhaseCommitCoordinator(DatabaseConfig config) {
    return new ConsensusCommitCoordinator(config);
  }

  @Override
  public TwoPhaseCommitParticipant createRawTwoPhaseCommitParticipant(DatabaseConfig config) {
    return new ConsensusCommitParticipant(config);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Consensus commit does not hand out a {@link GlobalTransactionManager} directly. This always
   * throws {@link UnsupportedOperationException}.
   */
  @Override
  public GlobalTransactionManager createGlobalTransactionManager(DatabaseConfig config) {
    throw new UnsupportedOperationException(
        CoreError.CONSENSUS_COMMIT_GLOBAL_TRANSACTION_NOT_SUPPORTED.buildMessage());
  }
}
