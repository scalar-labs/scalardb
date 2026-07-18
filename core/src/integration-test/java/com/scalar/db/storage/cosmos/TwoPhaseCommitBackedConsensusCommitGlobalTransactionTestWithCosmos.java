package com.scalar.db.storage.cosmos;

import com.scalar.db.transaction.consensuscommit.TwoPhaseCommitBackedConsensusCommitGlobalTransactionTestBase;
import java.util.Map;
import java.util.Properties;

public class TwoPhaseCommitBackedConsensusCommitGlobalTransactionTestWithCosmos
    extends TwoPhaseCommitBackedConsensusCommitGlobalTransactionTestBase {

  @Override
  protected Properties getProps(String testName) {
    return ConsensusCommitCosmosEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return ConsensusCommitCosmosEnv.getCreationOptions();
  }
}
