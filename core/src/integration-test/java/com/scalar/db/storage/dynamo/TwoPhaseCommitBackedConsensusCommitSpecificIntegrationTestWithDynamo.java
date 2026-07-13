package com.scalar.db.storage.dynamo;

import com.scalar.db.transaction.consensuscommit.TwoPhaseCommitBackedConsensusCommitSpecificIntegrationTestBase;
import java.util.Map;
import java.util.Properties;

public class TwoPhaseCommitBackedConsensusCommitSpecificIntegrationTestWithDynamo
    extends TwoPhaseCommitBackedConsensusCommitSpecificIntegrationTestBase {

  @Override
  protected Properties getFacadeStorageProperties(String testName) {
    return ConsensusCommitDynamoEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return ConsensusCommitDynamoEnv.getCreationOptions();
  }
}
