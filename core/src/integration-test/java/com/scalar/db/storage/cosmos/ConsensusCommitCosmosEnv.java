package com.scalar.db.storage.cosmos;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTestUtils;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitTestUtils;
import java.util.Map;
import java.util.Properties;

public final class ConsensusCommitCosmosEnv {
  private ConsensusCommitCosmosEnv() {}

  public static Properties getProperties(String testName) {
    Properties properties = CosmosEnv.getProperties(testName);

    // Add testName as a coordinator schema suffix
    ConsensusCommitIntegrationTestUtils.addSuffixToCoordinatorNamespace(properties, testName);

    return ConsensusCommitTestUtils.loadConsensusCommitProperties(properties);
  }

  public static Map<String, String> getCreationOptions() {
    return CosmosEnv.getCreationOptions();
  }
}
