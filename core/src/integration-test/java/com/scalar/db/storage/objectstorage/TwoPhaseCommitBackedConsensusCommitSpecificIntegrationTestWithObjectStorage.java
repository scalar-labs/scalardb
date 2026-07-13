package com.scalar.db.storage.objectstorage;

import com.scalar.db.transaction.consensuscommit.TwoPhaseCommitBackedConsensusCommitSpecificIntegrationTestBase;
import java.util.Properties;

public class TwoPhaseCommitBackedConsensusCommitSpecificIntegrationTestWithObjectStorage
    extends TwoPhaseCommitBackedConsensusCommitSpecificIntegrationTestBase {

  @Override
  protected Properties getFacadeStorageProperties(String testName) {
    return ConsensusCommitObjectStorageEnv.getProperties(testName);
  }
}
