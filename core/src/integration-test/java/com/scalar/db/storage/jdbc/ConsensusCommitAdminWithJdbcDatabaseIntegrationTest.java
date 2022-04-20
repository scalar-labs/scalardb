package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminIntegrationTestBase;

public class ConsensusCommitAdminWithJdbcDatabaseIntegrationTest
    extends ConsensusCommitAdminIntegrationTestBase {

  @Override
  protected DatabaseConfig getDbConfig() {
    return JdbcEnv.getJdbcConfig();
  }
}
