package com.scalar.db.storage.cassandra;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitIntegrationTestBase;

public class TwoPhaseConsensusCommitIntegrationTestWithCassandra
    extends TwoPhaseConsensusCommitIntegrationTestBase {
  @Override
  protected DatabaseConfig getDbConfig() {
    return CassandraEnv.getDatabaseConfig();
  }
}
