package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.sql.SqlStatementWithTwoPhaseCommitTransactionSessionIntegrationTestBase;

public class SqlStatementWithTwoPhaseCommitTransactionSessionIntegrationTestWithJdbcDatabase
    extends SqlStatementWithTwoPhaseCommitTransactionSessionIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return JdbcEnv.getJdbcConfig();
  }
}
