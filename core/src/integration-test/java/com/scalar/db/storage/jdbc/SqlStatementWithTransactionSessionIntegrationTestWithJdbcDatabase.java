package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.sql.SqlStatementWithTransactionSessionIntegrationTestBase;

public class SqlStatementWithTransactionSessionIntegrationTestWithJdbcDatabase
    extends SqlStatementWithTransactionSessionIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return JdbcEnv.getJdbcConfig();
  }
}
