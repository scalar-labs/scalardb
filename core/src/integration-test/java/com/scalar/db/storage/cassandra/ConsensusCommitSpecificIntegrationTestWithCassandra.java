package com.scalar.db.storage.cassandra;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitSpecificIntegrationTestBase;
import java.util.Properties;

public class ConsensusCommitSpecificIntegrationTestWithCassandra
    extends ConsensusCommitSpecificIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return ConsensusCommitCassandraEnv.getProperties(testName);
  }
}
