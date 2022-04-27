package com.scalar.db.storage.cassandra;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.sql.SqlStatementWithTransactionSessionIntegrationTestBase;

public class SqlStatementWithTransactionSessionIntegrationTestWithCassandra
    extends SqlStatementWithTransactionSessionIntegrationTestBase {
  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CassandraEnv.getDatabaseConfig();
  }
}
