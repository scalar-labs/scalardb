package com.scalar.db.storage.dynamo;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitWithIncludeMetadataEnabledIntegrationTestBase;
import java.util.Map;
import java.util.Properties;

public class ConsensusCommitWithIncludeMetadataEnabledIntegrationTestWithDynamo
    extends ConsensusCommitWithIncludeMetadataEnabledIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return DynamoEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
  }
}
