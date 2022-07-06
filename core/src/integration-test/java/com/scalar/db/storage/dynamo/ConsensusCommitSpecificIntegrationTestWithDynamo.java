package com.scalar.db.storage.dynamo;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitSpecificIntegrationTestBase;
import java.util.Map;
import java.util.Properties;

public class ConsensusCommitSpecificIntegrationTestWithDynamo
    extends ConsensusCommitSpecificIntegrationTestBase {

  @Override
  protected Properties getProperties() {
    return DynamoEnv.getProperties();
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
  }
}
