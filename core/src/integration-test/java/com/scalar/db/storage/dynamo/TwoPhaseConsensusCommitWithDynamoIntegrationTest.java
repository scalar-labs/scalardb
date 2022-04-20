package com.scalar.db.storage.dynamo;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitIntegrationTestBase;
import java.util.Map;

public class TwoPhaseConsensusCommitWithDynamoIntegrationTest
    extends TwoPhaseConsensusCommitIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return DynamoEnv.getDynamoConfig();
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return DynamoEnv.getCreateOptions();
  }
}
