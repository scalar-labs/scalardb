package com.scalar.db.storage.jdbc;

import com.google.errorprone.annotations.concurrent.LazyInit;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderWithMetadataDecouplingIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;

public class JdbcSchemaLoaderWithMetadataDecouplingIntegrationTest
    extends SchemaLoaderWithMetadataDecouplingIntegrationTestBase {
  @LazyInit private RdbEngineStrategy rdbEngine;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = ConsensusCommitJdbcEnv.getProperties(testName);
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);

    // Set the isolation level for consistency reads for virtual tables
    properties.setProperty(
        JdbcConfig.ISOLATION_LEVEL,
        JdbcTestUtils.getIsolationLevel(
                rdbEngine.getMinimumIsolationLevelForConsistentVirtualTableRead())
            .name());

    return properties;
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new JdbcAdminTestUtils(getProperties(testName));
  }
}
