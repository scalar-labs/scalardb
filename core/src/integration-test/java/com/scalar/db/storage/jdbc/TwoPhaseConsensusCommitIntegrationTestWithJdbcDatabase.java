package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitIntegrationTestBase;

public class TwoPhaseConsensusCommitIntegrationTestWithJdbcDatabase
    extends TwoPhaseConsensusCommitIntegrationTestBase {

  @Override
  protected DatabaseConfig getDbConfig() {
    return JdbcEnv.getJdbcConfig();
  }
}
