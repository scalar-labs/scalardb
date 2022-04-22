package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTestBase;

public class ConsensusCommitIntegrationTestWithJdbcDatabase
    extends ConsensusCommitIntegrationTestBase {

  @Override
  protected DatabaseConfig getDbConfig() {
    return JdbcEnv.getJdbcConfig();
  }
}
