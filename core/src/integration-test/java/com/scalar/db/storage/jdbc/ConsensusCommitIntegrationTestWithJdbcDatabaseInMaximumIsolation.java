package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTestBase;
import java.util.Properties;

public class ConsensusCommitIntegrationTestWithJdbcDatabaseInMaximumIsolation
    extends ConsensusCommitIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    Properties properties = ConsensusCommitJdbcEnv.getProperties(testName);

    // Set the isolation level to the maximum level
    RdbEngineStrategy rdbEngine =
        RdbEngineFactory.create(new JdbcConfig(new DatabaseConfig(properties)));
    properties.setProperty(
        JdbcConfig.ISOLATION_LEVEL,
        JdbcTestUtils.getIsolationLevel(rdbEngine.getMaximumIsolationLevel()).name());

    return properties;
  }
}
