package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcEnv;
import com.scalar.db.transaction.TransactionAdminIntegrationTestBase;

public class ConsensusCommitAdminWithJdbcDatabaseIntegrationTest
    extends TransactionAdminIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return JdbcEnv.getJdbcConfig();
  }
}
