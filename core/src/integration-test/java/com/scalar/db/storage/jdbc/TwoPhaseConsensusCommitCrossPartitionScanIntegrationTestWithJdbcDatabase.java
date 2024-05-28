package com.scalar.db.storage.jdbc;

import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitCrossPartitionScanIntegrationTestBase;
import java.util.Properties;

public class TwoPhaseConsensusCommitCrossPartitionScanIntegrationTestWithJdbcDatabase
    extends TwoPhaseConsensusCommitCrossPartitionScanIntegrationTestBase {

  @Override
  protected Properties getProps1(String testName) {
    return ConsensusCommitJdbcEnv.getProperties(testName);
  }
}
