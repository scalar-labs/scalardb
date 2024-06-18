package com.scalar.db.storage.cosmos;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTestBase;
import java.util.Map;
import java.util.Properties;

public class ConsensusCommitIntegrationTestWithCosmos extends ConsensusCommitIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return CosmosEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return CosmosEnv.getCreationOptions();
  }
}
