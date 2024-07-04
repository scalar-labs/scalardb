package com.scalar.db.storage.dynamo;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTestBase;
import java.util.Map;
import java.util.Properties;

public class ConsensusCommitIntegrationTestWithDynamo extends ConsensusCommitIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return ConsensusCommitDynamoEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return ConsensusCommitDynamoEnv.getCreationOptions();
  }
}
