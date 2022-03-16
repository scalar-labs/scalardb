package com.scalar.db.transaction.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.jdbc.JdbcEnv;
import com.scalar.db.transaction.TransactionAdminIntegrationTestBase;
import java.util.Properties;

public class JdbcTransactionAdminIntegrationTest extends TransactionAdminIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    JdbcConfig jdbcConfig = JdbcEnv.getJdbcConfig();
    Properties properties = new Properties();
    properties.putAll(jdbcConfig.getProperties());
    properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "jdbc");
    return new DatabaseConfig(properties);
  }
}
