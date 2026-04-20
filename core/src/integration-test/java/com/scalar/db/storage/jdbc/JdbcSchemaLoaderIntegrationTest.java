package com.scalar.db.storage.jdbc;

import com.google.common.util.concurrent.Uninterruptibles;
import com.google.errorprone.annotations.concurrent.LazyInit;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

public class JdbcSchemaLoaderIntegrationTest extends SchemaLoaderIntegrationTestBase {
  @LazyInit private RdbEngineStrategy rdbEngine;

  @Override
  protected Properties getProperties(String testName) {
    return ConsensusCommitJdbcEnv.getProperties(testName);
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
    if (JdbcTestUtils.isSpanner(rdbEngine)) {
      // Spanner DDL operations are eventually consistent on the emulator.
      // A short delay prevents "Duplicate name in schema" errors when
      // creating a schema that was just dropped by a previous test.
      Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
      return;
    }
    super.waitForCreationIfNecessary();
  }

  @DisabledIf("com.scalar.db.storage.jdbc.JdbcEnv#isSpanner")
  @Test
  @Override
  public void createTableThenAlterTables_ShouldExecuteProperly() throws Exception {
    // Spanner PG does not support creating secondary indexes on FLOAT32/FLOAT64 columns.
    // The altered schema adds a secondary index on col3 (FLOAT) which Spanner rejects.
    super.createTableThenAlterTables_ShouldExecuteProperly();
  }

  // Reading namespace information right after table deletion could fail with YugabyteDB.
  @Override
  protected boolean couldFailToReadNamespaceAfterDeletingTable() {
    if (JdbcTestUtils.isYugabyte(rdbEngine) || JdbcTestUtils.isSpanner(rdbEngine)) {
      return true;
    }
    return super.couldFailToReadNamespaceAfterDeletingTable();
  }
}
