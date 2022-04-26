package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitSpecificIntegrationTestBase;

public class ConsensusCommitSpecificIntegrationTestWithJdbcDatabase
    extends ConsensusCommitSpecificIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return JdbcEnv.getJdbcConfig();
  }
}
