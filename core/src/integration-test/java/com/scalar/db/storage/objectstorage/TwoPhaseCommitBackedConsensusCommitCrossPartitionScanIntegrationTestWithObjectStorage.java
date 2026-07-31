package com.scalar.db.storage.objectstorage;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.TwoPhaseCommitBackedConsensusCommitCrossPartitionScanIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class TwoPhaseCommitBackedConsensusCommitCrossPartitionScanIntegrationTestWithObjectStorage
    extends TwoPhaseCommitBackedConsensusCommitCrossPartitionScanIntegrationTestBase {

  @Override
  protected Properties getFacadeStorageProperties(String testName) {
    Properties properties = ConsensusCommitObjectStorageEnv.getProperties(testName);
    properties.setProperty(ConsensusCommitConfig.ISOLATION_LEVEL, "SERIALIZABLE");
    return properties;
  }

  @Test
  @Override
  @Disabled("Cross-partition scan with ordering is not supported in Object Storage")
  public void scan_CrossPartitionScanWithOrderingGivenForCommittedRecord_ShouldReturnRecords() {}
}
