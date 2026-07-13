package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TwoPhaseCommit;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.common.AbstractDistributedTransactionProvider;
import com.scalar.db.common.TwoPhaseCommitBackedDistributedTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 * A test-only {@link com.scalar.db.api.DistributedTransactionProvider} that exposes the
 * consensus-commit-backed {@link TwoPhaseCommit.Coordinator} / {@link TwoPhaseCommit.Participant}
 * roles through the single-phase {@link TwoPhaseCommitBackedDistributedTransactionManager} facade.
 *
 * <p>It is registered via {@code META-INF/services} in the integration-test module only, so it is
 * on the classpath when the integration tests run but never bundled into the production {@code
 * core} artifact. Selecting it (via {@code scalar.db.transaction_manager = }{@value #NAME}) makes
 * the existing {@code DistributedTransaction} integration-test corpus run against the new
 * Coordinator / Participant code path.
 *
 * <p>{@link ConsensusCommitConfig} requires {@code scalar.db.transaction_manager} to be {@code
 * consensus-commit}, but routing to this provider requires the distinct name {@value #NAME}. So the
 * consensus-commit objects (coordinator, participant, admin) are built from a copy of the
 * properties with the transaction-manager name forced back to {@code consensus-commit}.
 */
public class TwoPhaseCommitBackedConsensusCommitProvider
    extends AbstractDistributedTransactionProvider {

  /** The {@code scalar.db.transaction_manager} value that selects this provider. */
  public static final String NAME = "consensus-commit-two-phase-commit-backed";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public DistributedTransactionManager createRawDistributedTransactionManager(
      DatabaseConfig config) {
    DatabaseConfig ccConfig = toConsensusCommitConfig(config);
    TwoPhaseCommit.Coordinator coordinator = new ConsensusCommitCoordinator(ccConfig);
    TwoPhaseCommit.Participant participant = new ConsensusCommitParticipant(ccConfig);
    return new TwoPhaseCommitBackedDistributedTransactionManager(
        ccConfig, coordinator, participant);
  }

  @Override
  public DistributedTransactionAdmin createDistributedTransactionAdmin(DatabaseConfig config) {
    return new ConsensusCommitAdmin(toConsensusCommitConfig(config));
  }

  @Nullable
  @Override
  public TwoPhaseCommitTransactionManager createRawTwoPhaseCommitTransactionManager(
      DatabaseConfig config) {
    // Not exercised by these integration tests; the facade is reached via the distributed-manager
    // factory method above.
    return null;
  }

  @Override
  public TwoPhaseCommit.Coordinator createRawTwoPhaseCommitCoordinator(DatabaseConfig config) {
    // Not exercised by these integration tests; the facade is reached via the distributed-manager
    // factory method above.
    throw new UnsupportedOperationException();
  }

  @Override
  public TwoPhaseCommit.Participant createRawTwoPhaseCommitParticipant(DatabaseConfig config) {
    // Not exercised by these integration tests; the facade is reached via the distributed-manager
    // factory method above.
    throw new UnsupportedOperationException();
  }

  // Returns a config whose transaction-manager name is forced to consensus-commit so the
  // consensus-commit objects accept it, while this provider is still selected by its own name.
  private static DatabaseConfig toConsensusCommitConfig(DatabaseConfig config) {
    Properties properties = new Properties();
    properties.putAll(config.getProperties());
    properties.setProperty(
        DatabaseConfig.TRANSACTION_MANAGER, ConsensusCommitConfig.TRANSACTION_MANAGER_NAME);
    return new DatabaseConfig(properties);
  }
}
