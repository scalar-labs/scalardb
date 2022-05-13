package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.DistributedTransactionAdminIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;

public abstract class ConsensusCommitAdminIntegrationTestBase
    extends DistributedTransactionAdminIntegrationTestBase {

  @Override
  protected final Properties gerProperties() {
    Properties properties = new Properties();
    properties.putAll(getProps());
    properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "consensus-commit");
    return properties;
  }

  protected abstract Properties getProps();
}
