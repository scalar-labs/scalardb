package com.scalar.db.transaction.jdbc;

import com.scalar.db.api.DistributedTransactionAdminRepairTableIntegrationTestBase;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcAdminTestUtils;
import com.scalar.db.storage.jdbc.JdbcEnv;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;

public class JdbcTransactionAdminRepairTableIntegrationTest
    extends DistributedTransactionAdminRepairTableIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = new Properties();
    properties.putAll(JdbcEnv.getProperties(testName));
    properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "jdbc");
    return properties;
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new JdbcAdminTestUtils(JdbcEnv.getProperties(testName));
  }

  @Override
  protected boolean hasCoordinatorTables() {
    return false;
  }
}
