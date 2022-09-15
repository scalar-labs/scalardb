package com.scalar.db.storage.jdbc;

import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitSpecificIntegrationTestBase;
import java.util.Properties;

public class TwoPhaseConsensusCommitSpecificIntegrationTestWithJdbcDatabase
    extends TwoPhaseConsensusCommitSpecificIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return JdbcEnv.getProperties(testName);
  }
}
