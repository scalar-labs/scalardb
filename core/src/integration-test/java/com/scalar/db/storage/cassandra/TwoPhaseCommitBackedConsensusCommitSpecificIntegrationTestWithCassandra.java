package com.scalar.db.storage.cassandra;

import com.scalar.db.transaction.consensuscommit.TwoPhaseCommitBackedConsensusCommitSpecificIntegrationTestBase;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class TwoPhaseCommitBackedConsensusCommitSpecificIntegrationTestWithCassandra
    extends TwoPhaseCommitBackedConsensusCommitSpecificIntegrationTestBase {

  @Override
  protected Properties getFacadeStorageProperties(String testName) {
    return ConsensusCommitCassandraEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return Collections.singletonMap(CassandraAdmin.REPLICATION_FACTOR, "1");
  }
}
