package com.scalar.db.storage.cosmos;

import com.scalar.db.transaction.consensuscommit.TwoPhaseCommitBackedConsensusCommitIntegrationTestBase;
import java.util.Map;
import java.util.Properties;

public class TwoPhaseCommitBackedConsensusCommitIntegrationTestWithCosmos
    extends TwoPhaseCommitBackedConsensusCommitIntegrationTestBase {

  @Override
  protected Properties getFacadeStorageProperties(String testName) {
    return ConsensusCommitCosmosEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return ConsensusCommitCosmosEnv.getCreationOptions();
  }
}
