package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitSpecificWithMetadataDecouplingIntegrationTestBase;
import java.util.Properties;

public class ConsensusCommitSpecificWithMetadataDecouplingIntegrationTestWithJdbcDatabase
    extends ConsensusCommitSpecificWithMetadataDecouplingIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = ConsensusCommitJdbcEnv.getProperties(testName);

    // Set the isolation level for consistency reads for virtual tables
    RdbEngineStrategy rdbEngine =
        RdbEngineFactory.create(new JdbcConfig(new DatabaseConfig(properties)));
    properties.setProperty(
        JdbcConfig.ISOLATION_LEVEL,
        JdbcTestUtils.getIsolationLevel(
                rdbEngine.getMinimumIsolationLevelForConsistentVirtualTableRead())
            .name());

    // Disable connection pooling for Oracle to avoid test failures.
    // Oracle's SERIALIZABLE isolation level uses snapshot isolation, and reusing connections can
    // cause unexpected behavior due to stale snapshot state from previous transactions.
    if (JdbcEnv.isOracle()) {
      properties.setProperty(JdbcConfig.CONNECTION_POOL_MIN_IDLE, "0");
      properties.setProperty(JdbcConfig.CONNECTION_POOL_MAX_IDLE, "0");
      properties.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MIN_IDLE, "0");
      properties.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MAX_IDLE, "0");
      properties.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MIN_IDLE, "0");
      properties.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MAX_IDLE, "0");
    }

    return properties;
  }
}
