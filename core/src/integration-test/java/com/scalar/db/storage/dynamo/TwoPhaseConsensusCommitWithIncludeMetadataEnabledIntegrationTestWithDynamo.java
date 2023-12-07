package com.scalar.db.storage.dynamo;

import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitWithIncludeMetadataEnabledIntegrationTestBase;
import java.util.Map;
import java.util.Properties;

public class TwoPhaseConsensusCommitWithIncludeMetadataEnabledIntegrationTestWithDynamo
    extends TwoPhaseConsensusCommitWithIncludeMetadataEnabledIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return DynamoEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
  }
}
