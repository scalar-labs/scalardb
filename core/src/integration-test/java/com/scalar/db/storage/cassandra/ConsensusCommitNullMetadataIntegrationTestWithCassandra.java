package com.scalar.db.storage.cassandra;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitNullMetadataIntegrationTestBase;
import java.util.Properties;

public class ConsensusCommitNullMetadataIntegrationTestWithCassandra
    extends ConsensusCommitNullMetadataIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return ConsensusCommitCassandraEnv.getProperties(testName);
  }
}
