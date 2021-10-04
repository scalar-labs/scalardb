package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.cassandra.CassandraEnv;

public class ConsensusCommitWithCassandraIntegrationTest
    extends ConsensusCommitIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CassandraEnv.getDatabaseConfig();
  }
}
