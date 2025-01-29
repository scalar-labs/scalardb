package com.scalar.db.storage.cassandra;

import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitIntegrationTestBase;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class TwoPhaseConsensusCommitIntegrationTestWithCassandra
    extends TwoPhaseConsensusCommitIntegrationTestBase {
  @Override
  protected Properties getProps1(String testName) {
    return ConsensusCommitCassandraEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return Collections.singletonMap(CassandraAdmin.REPLICATION_FACTOR, "1");
  }

  @Override
  protected boolean isTimestampTypeSupported() {
    return false;
  }
}
