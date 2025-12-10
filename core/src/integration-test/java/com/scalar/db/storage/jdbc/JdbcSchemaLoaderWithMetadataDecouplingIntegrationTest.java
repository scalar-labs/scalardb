package com.scalar.db.storage.jdbc;

import com.google.common.util.concurrent.Uninterruptibles;
import com.google.errorprone.annotations.concurrent.LazyInit;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderWithMetadataDecouplingIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

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

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new JdbcAdminTestUtils(getProperties(testName));
  }

  @Override
  protected void waitForCreationIfNecessary() {
    if (JdbcTestUtils.isYugabyte(rdbEngine)) {
      Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
      return;
    }
    super.waitForCreationIfNecessary();
  }

  // Reading namespace information right after table deletion could fail with YugabyteDB.
  // It should be retried.
  @Override
  protected boolean couldFailToReadNamespaceAfterDeletingTable() {
    if (JdbcTestUtils.isYugabyte(rdbEngine)) {
      return true;
    }
    return super.couldFailToReadNamespaceAfterDeletingTable();
  }
}
