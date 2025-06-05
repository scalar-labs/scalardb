package com.scalar.db.storage.cassandra;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitTestUtils;
import java.util.Properties;

public final class ConsensusCommitCassandraEnv {
  private ConsensusCommitCassandraEnv() {}

  public static Properties getProperties(String testName) {
    Properties properties = CassandraEnv.getProperties(testName);

    // Add testName as a coordinator schema suffix
    ConsensusCommitTestUtils.addSuffixToCoordinatorNamespace(properties, testName);

    return ConsensusCommitTestUtils.loadConsensusCommitProperties(properties);
  }
}
