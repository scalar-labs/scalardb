package com.scalar.db.storage.jdbc;

import static com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig.COORDINATOR_GROUP_COMMIT_ENABLED;

import java.util.Properties;

public final class ConsensusCommitJdbcEnv {
  private static final String DEFAULT_GROUP_COMMIT_ENABLED = "false";

  private ConsensusCommitJdbcEnv() {}

  public static Properties getProperties(String testName) {
    String groupCommitEnabled =
        System.getProperty(COORDINATOR_GROUP_COMMIT_ENABLED, DEFAULT_GROUP_COMMIT_ENABLED);

    Properties properties = JdbcEnv.getProperties(testName);
    properties.setProperty(COORDINATOR_GROUP_COMMIT_ENABLED, groupCommitEnabled);

    return properties;
  }
}
