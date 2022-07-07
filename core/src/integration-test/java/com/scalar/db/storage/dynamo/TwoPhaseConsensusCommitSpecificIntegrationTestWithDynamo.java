package com.scalar.db.storage.dynamo;

import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitSpecificIntegrationTestBase;
import java.util.Map;
import java.util.Properties;

public class TwoPhaseConsensusCommitSpecificIntegrationTestWithDynamo
    extends TwoPhaseConsensusCommitSpecificIntegrationTestBase {

  @Override
  protected Properties getProperties() {
    return DynamoEnv.getProperties();
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
  }
}
