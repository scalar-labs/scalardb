package com.scalar.db.storage.objectstorage;

import com.scalar.db.transaction.consensuscommit.TwoPhaseCommitBackedConsensusCommitGlobalTransactionTestBase;
import java.util.Properties;

public class TwoPhaseCommitBackedConsensusCommitGlobalTransactionTestWithObjectStorage
    extends TwoPhaseCommitBackedConsensusCommitGlobalTransactionTestBase {

  @Override
  protected Properties getProps(String testName) {
    return ConsensusCommitObjectStorageEnv.getProperties(testName);
  }
}
