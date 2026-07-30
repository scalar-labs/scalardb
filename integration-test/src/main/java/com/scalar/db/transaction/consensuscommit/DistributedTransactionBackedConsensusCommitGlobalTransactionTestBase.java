package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.GlobalTransactionTestBase;
import com.scalar.db.common.DistributedTransactionBackedGlobalTransactionManager;
import com.scalar.db.service.TransactionFactory;
import java.util.Properties;

/**
 * Runs the {@link GlobalTransactionTestBase} corpus against the consensus-commit implementation
 * with the single-phase {@link DistributedTransactionBackedGlobalTransactionManager} backing.
 *
 * <p>This is the fully-shared deployment: every branch is served by one underlying distributed
 * transaction on a single manager. {@link #manager1} and {@link #manager2} are therefore the same
 * instance, and a branch begun on either joins (by ID) the one transaction begun on it. Contrast
 * with {@link TwoPhaseCommitBackedConsensusCommitGlobalTransactionTestBase}, where two managers
 * coordinate across two participants via a shared coordinator.
 */
public abstract class DistributedTransactionBackedConsensusCommitGlobalTransactionTestBase
    extends GlobalTransactionTestBase {

  @Override
  protected String getTestName() {
    return "global_tx_cc_sp";
  }

  @Override
  protected final Properties getProperties(String testName) {
    return getProps(testName);
  }

  protected abstract Properties getProps(String testName);

  @Override
  protected void setUpManagers() {
    DistributedTransactionManager transactionManager =
        TransactionFactory.create(getProps(getTestName())).getTransactionManager();
    // The fully-shared backing serves every branch from one underlying distributed transaction on a
    // single manager, so both handles are the same manager instance.
    manager1 = new DistributedTransactionBackedGlobalTransactionManager(transactionManager);
    manager2 = manager1;
  }

  @Override
  protected void tearDownManagers() {
    // manager1 and manager2 are the same instance, so close once.
    if (manager1 != null) {
      manager1.close();
    }
  }
}
