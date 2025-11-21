package com.scalar.db.storage.jdbc;

import com.google.common.util.concurrent.Uninterruptibles;
import com.google.errorprone.annotations.concurrent.LazyInit;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderWithMetadataDecouplingIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.condition.DisabledIf;

@DisabledIf("com.scalar.db.storage.jdbc.JdbcEnv#isOracle")
public class JdbcSchemaLoaderWithMetadataDecouplingIntegrationTest
    extends SchemaLoaderWithMetadataDecouplingIntegrationTestBase {
  @LazyInit private RdbEngineStrategy rdbEngine;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = ConsensusCommitJdbcEnv.getProperties(testName);
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);

    // Set the isolation level for consistency reads
    properties.setProperty(
        JdbcConfig.ISOLATION_LEVEL,
        JdbcTestUtils.getIsolationLevel(
                rdbEngine.getMinimumIsolationLevelForConsistentVirtualTableRead())
            .name());

    return properties;
  }

  @Override
  protected void initialize(String testName) throws Exception {
    super.initialize(testName);
    Properties properties = getProperties(testName);
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
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
