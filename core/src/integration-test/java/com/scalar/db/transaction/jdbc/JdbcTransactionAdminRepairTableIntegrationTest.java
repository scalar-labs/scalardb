package com.scalar.db.transaction.jdbc;

import com.scalar.db.api.DistributedTransactionAdminRepairTableIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcEnv;
import java.util.Properties;

public class JdbcTransactionAdminRepairTableIntegrationTest
    extends DistributedTransactionAdminRepairTableIntegrationTestBase {

  @Override
  protected Properties getProperties() {
    Properties properties = new Properties();
    properties.putAll(JdbcEnv.getProperties());
    properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "jdbc");
    return properties;
  }

  @Override
  protected boolean hasCoordinatorTables() {
    return false;
  }
}
