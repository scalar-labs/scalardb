package com.scalar.db.storage.dynamo;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTestBase;
import java.util.Map;

public class ConsensusCommitIntegrationTestWithDynamo extends ConsensusCommitIntegrationTestBase {

  @Override
  protected DatabaseConfig getDbConfig() {
    return DynamoEnv.getDynamoConfig();
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return DynamoEnv.getCreateOptions();
  }
}
