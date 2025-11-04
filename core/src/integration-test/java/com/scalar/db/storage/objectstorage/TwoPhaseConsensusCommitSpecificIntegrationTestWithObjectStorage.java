package com.scalar.db.storage.objectstorage;

import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitSpecificIntegrationTestBase;
import java.util.Properties;

public class TwoPhaseConsensusCommitSpecificIntegrationTestWithObjectStorage
    extends TwoPhaseConsensusCommitSpecificIntegrationTestBase {

  @Override
  protected Properties getProperties1(String testName) {
    return ConsensusCommitObjectStorageEnv.getProperties(testName);
  }
}
