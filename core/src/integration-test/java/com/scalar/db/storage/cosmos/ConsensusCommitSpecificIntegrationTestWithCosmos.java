package com.scalar.db.storage.cosmos;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitSpecificIntegrationTestBase;
import java.util.Map;
import java.util.Properties;

public class ConsensusCommitSpecificIntegrationTestWithCosmos
    extends ConsensusCommitSpecificIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return ConsensusCommitCosmosEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return ConsensusCommitCosmosEnv.getCreationOptions();
  }
}
