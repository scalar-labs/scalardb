package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;

/**
 * Runs the ConsensusCommit cross-partition-scan integration-test corpus against the {@link
 * com.scalar.db.api.TwoPhaseCommitCoordinator} / {@link
 * com.scalar.db.api.TwoPhaseCommitParticipant} roles, driven through the single-phase {@link
 * com.scalar.db.common.GlobalTransactionBackedDistributedTransactionManager} facade.
 *
 * <p>Like {@link TwoPhaseCommitBackedConsensusCommitIntegrationTestBase}, it reuses the
 * ConsensusCommit setup and only swaps the transaction manager via {@code getProps}. The
 * cross-partition-scan corpus does not use the by-id Coordinator-state operations, so no tests need
 * to be disabled here.
 */
public abstract class TwoPhaseCommitBackedConsensusCommitCrossPartitionScanIntegrationTestBase
    extends ConsensusCommitCrossPartitionScanIntegrationTestBase {

  @Override
  protected String getTestName() {
    return "tx_cross_scan_2pc_backed";
  }

  @Override
  protected final Properties getProps(String testName) {
    Properties properties = getFacadeStorageProperties(testName);
    properties.setProperty(
        DatabaseConfig.TRANSACTION_MANAGER, TwoPhaseCommitBackedConsensusCommitProvider.NAME);
    if (properties.getProperty(ConsensusCommitConfig.PARTICIPANT_ID) == null) {
      properties.setProperty(ConsensusCommitConfig.PARTICIPANT_ID, "participant-1");
    }
    return properties;
  }

  /** Storage-specific ConsensusCommit properties (typically from a {@code ConsensusCommit*Env}). */
  protected abstract Properties getFacadeStorageProperties(String testName);
}
