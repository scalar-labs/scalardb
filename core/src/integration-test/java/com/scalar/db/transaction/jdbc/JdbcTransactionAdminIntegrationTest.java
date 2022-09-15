package com.scalar.db.transaction.jdbc;

import com.scalar.db.api.DistributedTransactionAdminIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcEnv;
import java.util.Properties;

public class JdbcTransactionAdminIntegrationTest
    extends DistributedTransactionAdminIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = new Properties();
    properties.putAll(JdbcEnv.getProperties(testName));
    properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "jdbc");
    return properties;
  }

  @Override
  protected boolean hasCoordinatorTables() {
    return false;
  }

  @Override
  protected String getCoordinatorNamespace(String testName) {
    throw new UnsupportedOperationException();
  }
}
