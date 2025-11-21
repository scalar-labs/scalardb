package com.scalar.db.storage.jdbc;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitSpecificIntegrationTestBase;
import java.util.Properties;

public class ConsensusCommitSpecificIntegrationTestWithJdbcDatabase
    extends ConsensusCommitSpecificIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = ConsensusCommitJdbcEnv.getProperties(testName);
    properties.setProperty(JdbcConfig.ISOLATION_LEVEL, "SERIALIZABLE");
    return properties;
  }
}
