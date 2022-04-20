package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitIntegrationTestBase;

public class TwoPhaseConsensusCommitWithJdbcDatabaseIntegrationTest
    extends TwoPhaseConsensusCommitIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return JdbcEnv.getJdbcConfig();
  }
}
