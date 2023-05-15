package com.scalar.db.storage.dynamo;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitNullMetadataIntegrationTestBase;
import java.util.Map;
import java.util.Properties;

public class ConsensusCommitNullMetadataIntegrationTestWithDynamo
    extends ConsensusCommitNullMetadataIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return DynamoEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
  }
}
