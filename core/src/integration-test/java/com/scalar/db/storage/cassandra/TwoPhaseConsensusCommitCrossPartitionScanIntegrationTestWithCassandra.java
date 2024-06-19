package com.scalar.db.storage.cassandra;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitCrossPartitionScanIntegrationTestBase;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class TwoPhaseConsensusCommitCrossPartitionScanIntegrationTestWithCassandra
    extends TwoPhaseConsensusCommitCrossPartitionScanIntegrationTestBase {

  @Override
  protected Properties getProps1(String testName) {
    Properties properties = ConsensusCommitCassandraEnv.getProperties(testName);
    properties.setProperty(ConsensusCommitConfig.ISOLATION_LEVEL, "SERIALIZABLE");
    return properties;
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return Collections.singletonMap(CassandraAdmin.REPLICATION_FACTOR, "1");
  }

  @Test
  @Override
  @Disabled("Cross partition scan with ordering is not supported in Cassandra")
  public void scan_ScanWithOrderingGivenForCommittedRecord_ShouldReturnRecords() {}
}
