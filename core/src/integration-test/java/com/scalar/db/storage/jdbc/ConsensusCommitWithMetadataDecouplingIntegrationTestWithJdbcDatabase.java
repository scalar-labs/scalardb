package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitWithMetadataDecouplingIntegrationTestBase;
import java.util.Properties;

public class ConsensusCommitWithMetadataDecouplingIntegrationTestWithJdbcDatabase
    extends ConsensusCommitWithMetadataDecouplingIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    Properties properties = ConsensusCommitJdbcEnv.getProperties(testName);

    // Set the isolation level for consistency reads for virtual tables
    RdbEngineStrategy rdbEngine =
        RdbEngineFactory.create(new JdbcConfig(new DatabaseConfig(properties)));
    properties.setProperty(
        JdbcConfig.ISOLATION_LEVEL,
        JdbcTestUtils.getIsolationLevel(
                rdbEngine.getMinimumIsolationLevelForConsistentVirtualTableRead())
            .name());

    return properties;
  }
}
