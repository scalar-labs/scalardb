package com.scalar.db.storage.jdbc;

import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitRelationalScanIntegrationTestBase;
import java.util.Properties;

public class TwoPhaseConsensusCommitRelationalScanIntegrationTestWithJdbcDatabase
    extends TwoPhaseConsensusCommitRelationalScanIntegrationTestBase {

  @Override
  protected Properties getProps1(String testName) {
    return JdbcEnv.getProperties(testName);
  }
}
