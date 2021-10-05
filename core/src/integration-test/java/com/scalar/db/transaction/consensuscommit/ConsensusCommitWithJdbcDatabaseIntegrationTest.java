package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcEnv;

public class ConsensusCommitWithJdbcDatabaseIntegrationTest
    extends ConsensusCommitIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return JdbcEnv.getJdbcConfig();
  }
}
