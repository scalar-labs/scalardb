package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.dynamo.DynamoEnv;
import java.util.Map;

public class ConsensusCommitWithDynamoIntegrationTest extends ConsensusCommitIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return DynamoEnv.getDynamoConfig();
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return DynamoEnv.getCreateOptions();
  }
}
