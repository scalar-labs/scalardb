package com.scalar.db.storage.multistorage;

import com.scalar.db.common.ConsensusCommitTestUtils;
import java.util.Properties;

public final class ConsensusCommitMultiStorageEnv {
  private ConsensusCommitMultiStorageEnv() {}

  public static Properties getPropertiesForCassandra(@SuppressWarnings("unused") String testName) {
    Properties properties = MultiStorageEnv.getPropertiesForCassandra(testName);
    return ConsensusCommitTestUtils.loadConsensusCommitProperties(properties);
  }

  public static Properties getPropertiesForJdbc(String testName) {
    Properties properties = MultiStorageEnv.getPropertiesForJdbc(testName);
    return ConsensusCommitTestUtils.loadConsensusCommitProperties(properties);
  }
}
