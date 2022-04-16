package com.scalar.db.storage.cassandra;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminIntegrationTestBase;

public class ConsensusCommitAdminWithCassandraIntegrationTest
    extends ConsensusCommitAdminIntegrationTestBase {
  @Override
  protected DatabaseConfig getDbConfig() {
    return CassandraEnv.getDatabaseConfig();
  }
}
