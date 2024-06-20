package com.scalar.db.storage.cosmos;

import com.scalar.db.common.ConsensusCommitTestUtils;
import java.util.Map;
import java.util.Properties;

public final class ConsensusCommitCosmosEnv {
  private ConsensusCommitCosmosEnv() {}

  public static Properties getProperties(String testName) {
    Properties properties = CosmosEnv.getProperties(testName);
    return ConsensusCommitTestUtils.loadConsensusCommitProperties(properties);
  }

  public static Map<String, String> getCreationOptions() {
    return CosmosEnv.getCreationOptions();
  }
}
