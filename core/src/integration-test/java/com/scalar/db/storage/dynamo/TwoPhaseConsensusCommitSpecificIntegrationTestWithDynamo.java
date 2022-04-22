package com.scalar.db.storage.dynamo;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitSpecificIntegrationTestBase;
import java.util.Map;

public class TwoPhaseConsensusCommitSpecificIntegrationTestWithDynamo
    extends TwoPhaseConsensusCommitSpecificIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return DynamoEnv.getDynamoConfig();
  }

  @Override
  protected Map<String, String> getCreateOptions() {
    return DynamoEnv.getCreateOptions();
  }
}
