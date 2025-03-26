package com.scalar.db.storage.objectstorage;

import com.scalar.db.common.ConsensusCommitTestUtils;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTestUtils;
import java.util.Properties;

public class ConsensusCommitObjectStorageEnv {
  private ConsensusCommitObjectStorageEnv() {}

  public static Properties getProperties(String testName) {
    Properties properties = ObjectStorageEnv.getProperties(testName);

    // Add testName as a coordinator schema suffix
    ConsensusCommitIntegrationTestUtils.addSuffixToCoordinatorNamespace(properties, testName);

    return ConsensusCommitTestUtils.loadConsensusCommitProperties(properties);
  }
}
