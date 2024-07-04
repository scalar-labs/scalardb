package com.scalar.db.storage.cassandra;

import com.scalar.db.common.ConsensusCommitTestUtils;
import java.util.Properties;

public final class ConsensusCommitCassandraEnv {
  private ConsensusCommitCassandraEnv() {}

  public static Properties getProperties(String testName) {
    Properties properties = CassandraEnv.getProperties(testName);
    return ConsensusCommitTestUtils.loadConsensusCommitProperties(properties);
  }
}
