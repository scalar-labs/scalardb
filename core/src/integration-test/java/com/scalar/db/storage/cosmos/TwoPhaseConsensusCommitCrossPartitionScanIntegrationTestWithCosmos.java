package com.scalar.db.storage.cosmos;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitCrossPartitionScanIntegrationTestBase;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class TwoPhaseConsensusCommitCrossPartitionScanIntegrationTestWithCosmos
    extends TwoPhaseConsensusCommitCrossPartitionScanIntegrationTestBase {

  @Override
  protected Properties getProps1(String testName) {
    Properties properties = ConsensusCommitCosmosEnv.getProperties(testName);
    properties.setProperty(ConsensusCommitConfig.ISOLATION_LEVEL, "SERIALIZABLE");
    return properties;
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return ConsensusCommitCosmosEnv.getCreationOptions();
  }

  @Test
  @Override
  @Disabled("Cross partition scan with ordering is not supported in Cosmos DB")
  public void scan_ScanWithOrderingGivenForCommittedRecord_ShouldReturnRecords() {}
}
