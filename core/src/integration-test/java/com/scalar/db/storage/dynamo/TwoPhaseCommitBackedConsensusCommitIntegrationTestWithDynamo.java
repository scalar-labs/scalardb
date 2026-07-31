package com.scalar.db.storage.dynamo;

import com.scalar.db.transaction.consensuscommit.TwoPhaseCommitBackedConsensusCommitIntegrationTestBase;
import java.util.Map;
import java.util.Properties;

public class TwoPhaseCommitBackedConsensusCommitIntegrationTestWithDynamo
    extends TwoPhaseCommitBackedConsensusCommitIntegrationTestBase {

  @Override
  protected Properties getFacadeStorageProperties(String testName) {
    return ConsensusCommitDynamoEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return ConsensusCommitDynamoEnv.getCreationOptions();
  }
}
