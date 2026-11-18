package com.scalar.db.storage.objectstorage;

import com.scalar.db.transaction.consensuscommit.DistributedTransactionBackedConsensusCommitGlobalTransactionTestBase;
import java.util.Properties;

public class DistributedTransactionBackedConsensusCommitGlobalTransactionTestWithObjectStorage
    extends DistributedTransactionBackedConsensusCommitGlobalTransactionTestBase {

  @Override
  protected Properties getProps(String testName) {
    return ConsensusCommitObjectStorageEnv.getProperties(testName);
  }
}
