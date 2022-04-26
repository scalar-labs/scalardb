package com.scalar.db.storage.cassandra;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTestBase;

public class ConsensusCommitIntegrationTestWithCassandra
    extends ConsensusCommitIntegrationTestBase {
  @Override
  protected DatabaseConfig getDbConfig() {
    return CassandraEnv.getDatabaseConfig();
  }
}
