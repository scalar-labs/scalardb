package com.scalar.db.storage.jdbc;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTestBase;
import java.util.Properties;

public class ConsensusCommitIntegrationTestWithJdbcDatabase
    extends ConsensusCommitIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return ConsensusCommitJdbcEnv.getProperties(testName);
  }
}
