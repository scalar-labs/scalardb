package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitSpecificIntegrationTestBase;

public class TwoPhaseConsensusCommitSpecificIntegrationTestWithJdbcDatabase
    extends TwoPhaseConsensusCommitSpecificIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return JdbcEnv.getJdbcConfig();
  }
}
