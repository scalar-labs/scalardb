package com.scalar.db.storage.objectstorage;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.transaction.consensuscommit.DistributedTransactionBackedConsensusCommitGlobalTransactionTestBase;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class DistributedTransactionBackedConsensusCommitGlobalTransactionTestWithObjectStorage
    extends DistributedTransactionBackedConsensusCommitGlobalTransactionTestBase {

  @Override
  @BeforeEach
  public void setUp() throws Exception {
    super.setUp();
    if (ObjectStorageEnv.isCloudStorage()) {
      // Sleep to mitigate rate limit errors
      Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
    }
  }

  @AfterEach
  void tearDown() {
    if (ObjectStorageEnv.isCloudStorage()) {
      // Sleep to mitigate rate limit errors
      Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
    }
  }

  @Override
  protected Properties getProps(String testName) {
    return ConsensusCommitObjectStorageEnv.getProperties(testName);
  }
}
