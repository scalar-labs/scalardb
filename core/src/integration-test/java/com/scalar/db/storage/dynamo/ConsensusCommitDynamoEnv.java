package com.scalar.db.storage.dynamo;

import com.scalar.db.common.ConsensusCommitTestUtils;
import java.util.Map;
import java.util.Properties;

public final class ConsensusCommitDynamoEnv {
  private ConsensusCommitDynamoEnv() {}

  public static Properties getProperties(String testName) {
    Properties properties = DynamoEnv.getProperties(testName);
    return ConsensusCommitTestUtils.loadConsensusCommitProperties(properties);
  }

  public static Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
  }
}
