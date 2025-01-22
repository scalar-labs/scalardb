package com.scalar.db.storage.cassandra;

import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitIntegrationTestBase;
import java.util.Properties;

public class TwoPhaseConsensusCommitIntegrationTestWithCassandra
    extends TwoPhaseConsensusCommitIntegrationTestBase {
  @Override
  protected Properties getProps1(String testName) {
    return ConsensusCommitCassandraEnv.getProperties(testName);
  }

  @Override
  protected boolean isTimestampTypeSupported() {
    return false;
  }
}
