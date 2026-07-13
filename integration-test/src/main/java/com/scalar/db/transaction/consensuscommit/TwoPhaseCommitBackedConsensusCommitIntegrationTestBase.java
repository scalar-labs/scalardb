package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.TransactionException;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Runs the ConsensusCommit distributed-transaction integration-test corpus against the new {@link
 * com.scalar.db.api.TwoPhaseCommit} Coordinator / Participant roles, driven through the
 * single-phase {@link com.scalar.db.common.TwoPhaseCommitBackedDistributedTransactionManager}
 * facade.
 *
 * <p>It reuses the entire {@link ConsensusCommitIntegrationTestBase} setup (coordinator tables,
 * truncation helpers, CC-specific public tests) and only swaps the transaction manager: {@code
 * getProps} sets {@code scalar.db.transaction_manager} to {@link
 * TwoPhaseCommitBackedConsensusCommitProvider#NAME} so {@link
 * com.scalar.db.service.TransactionFactory} routes to the facade.
 *
 * <p>The facade does not support the by-id Coordinator-state operations ({@code getState}, {@code
 * abort(id)}, {@code rollback(id)}); the base tests that exercise them are disabled below (they
 * throw {@link UnsupportedOperationException}). Their underlying behavior is covered by the
 * Coordinator / Participant unit tests.
 *
 * <p>The {@code resume} tests are left enabled. Although the raw facade does not implement {@code
 * resume}, the manager returned by {@link com.scalar.db.service.TransactionFactory} is wrapped by
 * the (default-enabled) active-transaction-management decorator, which serves {@code resume} from
 * its own registry without delegating to the facade, so those tests run against the manager users
 * actually get.
 */
public abstract class TwoPhaseCommitBackedConsensusCommitIntegrationTestBase
    extends ConsensusCommitIntegrationTestBase {

  @Override
  protected String getTestName() {
    return "tx_2pc_backed";
  }

  @Override
  protected final Properties getProps(String testName) {
    Properties properties = getFacadeStorageProperties(testName);
    // Route TransactionFactory to the facade instead of the plain ConsensusCommitManager.
    properties.setProperty(
        DatabaseConfig.TRANSACTION_MANAGER, TwoPhaseCommitBackedConsensusCommitProvider.NAME);
    // The single in-process participant requires a participant ID.
    if (properties.getProperty(ConsensusCommitConfig.PARTICIPANT_ID) == null) {
      properties.setProperty(ConsensusCommitConfig.PARTICIPANT_ID, "participant-1");
    }
    // The facade's Coordinator always persists the write set, which requires the opt-in
    // tx_write_set Coordinator column (disabled by default on this branch).
    properties.setProperty(ConsensusCommitConfig.COORDINATOR_WRITE_SET_LOGGING_ENABLED, "true");
    return properties;
  }

  /** Storage-specific ConsensusCommit properties (typically from a {@code ConsensusCommit*Env}). */
  protected abstract Properties getFacadeStorageProperties(String testName);

  // --- Tests disabled: the facade does not support by-id Coordinator-state operations ---

  @Disabled("The two-phase-commit-backed facade does not support getState by transaction ID")
  @Override
  @Test
  public void getState_forSuccessfulTransaction_ShouldReturnCommittedState()
      throws TransactionException {}

  @Disabled("The two-phase-commit-backed facade does not support getState by transaction ID")
  @Override
  @Test
  public void getState_forFailedTransaction_ShouldReturnAbortedState()
      throws TransactionException {}

  @Disabled("The two-phase-commit-backed facade does not support abort by transaction ID")
  @Override
  @Test
  public void abort_forOngoingTransaction_ShouldAbortCorrectly() throws TransactionException {}

  @Disabled(
      "The two-phase-commit-backed facade does not support rollback/getState by transaction ID")
  @Override
  @Test
  public void rollback_forOngoingTransaction_ShouldRollbackCorrectly()
      throws TransactionException {}
}
