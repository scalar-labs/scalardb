package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.DistributedTransactionAdminRepairTableIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;

public abstract class ConsensusCommitAdminRepairTableIntegrationTestBase
    extends DistributedTransactionAdminRepairTableIntegrationTestBase {

  @Override
  protected final Properties getProperties() {
    Properties properties = new Properties();
    properties.putAll(getProps());
    String transactionManager = properties.getProperty(DatabaseConfig.TRANSACTION_MANAGER, "");
    if (!transactionManager.equals("grpc")) {
      properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "consensus-commit");
    }
    return properties;
  }

  protected abstract Properties getProps();
}
