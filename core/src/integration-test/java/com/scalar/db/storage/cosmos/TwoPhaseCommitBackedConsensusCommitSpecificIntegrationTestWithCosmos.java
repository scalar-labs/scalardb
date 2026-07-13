package com.scalar.db.storage.cosmos;

import com.scalar.db.transaction.consensuscommit.TwoPhaseCommitBackedConsensusCommitSpecificIntegrationTestBase;
import java.util.Map;
import java.util.Properties;

public class TwoPhaseCommitBackedConsensusCommitSpecificIntegrationTestWithCosmos
    extends TwoPhaseCommitBackedConsensusCommitSpecificIntegrationTestBase {

  @Override
  protected Properties getFacadeStorageProperties(String testName) {
    return ConsensusCommitCosmosEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return ConsensusCommitCosmosEnv.getCreationOptions();
  }
}
