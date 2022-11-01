package com.scalar.db.storage.jdbc;

import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitIntegrationTestBase;
import java.util.Properties;

public class TwoPhaseConsensusCommitIntegrationTestWithJdbcDatabase
    extends TwoPhaseConsensusCommitIntegrationTestBase {

  @Override
  protected Properties getProps1(String testName) {
    return JdbcEnv.getProperties(testName);
  }
}
