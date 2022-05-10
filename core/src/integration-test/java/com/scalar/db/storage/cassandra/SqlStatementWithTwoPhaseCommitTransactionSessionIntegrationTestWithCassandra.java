package com.scalar.db.storage.cassandra;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.sql.SqlStatementWithTwoPhaseCommitTransactionSessionIntegrationTestBase;

public class SqlStatementWithTwoPhaseCommitTransactionSessionIntegrationTestWithCassandra
    extends SqlStatementWithTwoPhaseCommitTransactionSessionIntegrationTestBase {
  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CassandraEnv.getDatabaseConfig();
  }
}
