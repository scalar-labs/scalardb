package com.scalar.db.storage.objectstorage;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitCrossPartitionScanIntegrationTestBase;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class ConsensusCommitCrossPartitionScanIntegrationTestWithObjectStorage
    extends ConsensusCommitCrossPartitionScanIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    Properties properties = ConsensusCommitObjectStorageEnv.getProperties(testName);
    properties.setProperty(ConsensusCommitConfig.ISOLATION_LEVEL, "SERIALIZABLE");
    return properties;
  }

  @Override
  protected void waitToAvoidRateLimiting() {
    if (ObjectStorageEnv.isCloudStorage()) {
      Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
    }
  }

  @Test
  @Override
  @Disabled("Cross-partition scan with ordering is not supported in Object Storage")
  public void scan_CrossPartitionScanWithOrderingGivenForCommittedRecord_ShouldReturnRecords() {}
}
