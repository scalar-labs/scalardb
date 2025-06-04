package com.scalar.db.storage.jdbc;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitTestUtils;
import java.util.Properties;

public final class ConsensusCommitJdbcEnv {
  private ConsensusCommitJdbcEnv() {}

  public static Properties getProperties(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    return ConsensusCommitTestUtils.loadConsensusCommitProperties(properties);
  }
}
