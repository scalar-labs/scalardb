package com.scalar.db.storage.jdbc;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminIntegrationTestBase;
import java.util.Properties;

public class ConsensusCommitAdminIntegrationTestWithJdbcDatabase
    extends ConsensusCommitAdminIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return JdbcEnv.getProperties(testName);
  }
}
