package com.scalar.db.storage.dynamo;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitCrossPartitionScanIntegrationTestBase;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class TwoPhaseConsensusCommitCrossPartitionScanIntegrationTestWithDynamo
    extends TwoPhaseConsensusCommitCrossPartitionScanIntegrationTestBase {

  @Override
  protected Properties getProps1(String testName) {
    Properties properties = DynamoEnv.getProperties(testName);
    properties.setProperty(ConsensusCommitConfig.ISOLATION_LEVEL, "SERIALIZABLE");
    return properties;
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return DynamoEnv.getCreationOptions();
  }

  @Test
  @Override
  @Disabled
  public void scan_ScanWithOrderingGivenForCommittedRecord_ShouldReturnRecords() {}
}
