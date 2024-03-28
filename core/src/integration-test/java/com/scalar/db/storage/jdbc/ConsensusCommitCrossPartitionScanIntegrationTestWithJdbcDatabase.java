package com.scalar.db.storage.jdbc;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitCrossPartitionScanIntegrationTestBase;
import java.util.Properties;

public class ConsensusCommitCrossPartitionScanIntegrationTestWithJdbcDatabase
    extends ConsensusCommitCrossPartitionScanIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return ConsensusCommitJdbcEnv.getProperties(testName);
  }
}
