package com.scalar.db.storage.jdbc;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminRepairIntegrationTestBase;
import java.util.Properties;

public class ConsensusCommitAdminRepairIntegrationTestWithJdbcDatabase
    extends ConsensusCommitAdminRepairIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return JdbcEnv.getProperties(testName);
  }

  @Override
  protected void initialize(String testName) throws Exception {
    super.initialize(testName);
    adminTestUtils = new JdbcAdminTestUtils(getProperties(testName));
  }
}
