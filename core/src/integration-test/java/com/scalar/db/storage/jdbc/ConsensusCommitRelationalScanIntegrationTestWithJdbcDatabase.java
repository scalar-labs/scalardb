package com.scalar.db.storage.jdbc;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitRelationalScanIntegrationTestBase;
import java.util.Properties;

public class ConsensusCommitRelationalScanIntegrationTestWithJdbcDatabase
    extends ConsensusCommitRelationalScanIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return JdbcEnv.getProperties(testName);
  }
}
