package com.scalar.db.storage.objectstorage;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitTestUtils;
import java.util.Map;
import java.util.Properties;

public class ConsensusCommitObjectStorageEnv {
  private ConsensusCommitObjectStorageEnv() {}

  public static Properties getProperties(String testName) {
    Properties properties = ObjectStorageEnv.getProperties(testName);

    // Add testName as a coordinator schema suffix
    ConsensusCommitTestUtils.addSuffixToCoordinatorNamespace(properties, testName);

    return ConsensusCommitTestUtils.loadConsensusCommitProperties(properties);
  }

  public static Map<String, String> getCreationOptions() {
    return ObjectStorageEnv.getCreationOptions();
  }
}
