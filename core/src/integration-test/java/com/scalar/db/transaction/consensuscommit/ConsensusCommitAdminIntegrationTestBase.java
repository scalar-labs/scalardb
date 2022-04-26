package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.DistributedTransactionAdminIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;

public abstract class ConsensusCommitAdminIntegrationTestBase
    extends DistributedTransactionAdminIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    DatabaseConfig config = getDbConfig();
    Properties properties = new Properties();
    properties.putAll(config.getProperties());
    properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "consensus-commit");
    return new DatabaseConfig(properties);
  }

  protected abstract DatabaseConfig getDbConfig();
}
