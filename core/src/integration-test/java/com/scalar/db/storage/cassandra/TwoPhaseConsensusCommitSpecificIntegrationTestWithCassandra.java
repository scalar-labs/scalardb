package com.scalar.db.storage.cassandra;

import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitSpecificIntegrationTestBase;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class TwoPhaseConsensusCommitSpecificIntegrationTestWithCassandra
    extends TwoPhaseConsensusCommitSpecificIntegrationTestBase {

  @Override
  protected Properties getProperties1(String testName) {
    return ConsensusCommitCassandraEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return Collections.singletonMap(CassandraAdmin.REPLICATION_FACTOR, "1");
  }
}
