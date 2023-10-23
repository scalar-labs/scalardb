package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.DistributedTransactionAdminRepairIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;

public abstract class ConsensusCommitAdminRepairIntegrationTestBase
    extends DistributedTransactionAdminRepairIntegrationTestBase {

  @Override
  protected final Properties getProperties(String testName) {
    Properties properties = new Properties();
    properties.putAll(getProps(testName));
    if (!properties.containsKey(DatabaseConfig.TRANSACTION_MANAGER)) {
      properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "consensus-commit");

      // Add testName as a coordinator namespace suffix
      String coordinatorNamespace =
          properties.getProperty(
              ConsensusCommitConfig.COORDINATOR_NAMESPACE, Coordinator.NAMESPACE);
      properties.setProperty(
          ConsensusCommitConfig.COORDINATOR_NAMESPACE, coordinatorNamespace + "_" + testName);
    }
    return properties;
  }

  protected abstract Properties getProps(String testName);
}
