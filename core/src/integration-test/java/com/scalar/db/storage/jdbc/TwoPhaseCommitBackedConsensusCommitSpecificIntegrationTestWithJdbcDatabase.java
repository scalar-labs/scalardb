package com.scalar.db.storage.jdbc;

import com.scalar.db.transaction.consensuscommit.TwoPhaseCommitBackedConsensusCommitSpecificIntegrationTestBase;
import java.util.Properties;

public class TwoPhaseCommitBackedConsensusCommitSpecificIntegrationTestWithJdbcDatabase
    extends TwoPhaseCommitBackedConsensusCommitSpecificIntegrationTestBase {

  @Override
  protected Properties getFacadeStorageProperties(String testName) {
    return ConsensusCommitJdbcEnv.getProperties(testName);
  }
}
