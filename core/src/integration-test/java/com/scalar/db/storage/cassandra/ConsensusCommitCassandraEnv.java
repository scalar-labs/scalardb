package com.scalar.db.storage.cassandra;

import com.scalar.db.common.ConsensusCommitTestUtils;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTestUtils;
import java.util.Properties;

public final class ConsensusCommitCassandraEnv {
  private ConsensusCommitCassandraEnv() {}

  public static Properties getProperties(String testName) {
    Properties properties = CassandraEnv.getProperties(testName);

    // Add testName as a coordinator schema suffix
    ConsensusCommitIntegrationTestUtils.addSuffixToCoordinatorNamespace(properties, testName);

    return ConsensusCommitTestUtils.loadConsensusCommitProperties(properties);
  }
}
