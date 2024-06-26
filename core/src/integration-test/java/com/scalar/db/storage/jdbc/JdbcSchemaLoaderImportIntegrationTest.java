package com.scalar.db.storage.jdbc;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderImportIntegrationTestBase;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcSchemaLoaderImportIntegrationTest extends SchemaLoaderImportIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(JdbcSchemaLoaderImportIntegrationTest.class);

  private JdbcAdminImportTestUtils testUtils;
  private RdbEngineStrategy rdbEngine;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = new Properties();
    properties.putAll(JdbcEnv.getProperties(testName));
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
    testUtils = new JdbcAdminImportTestUtils(properties);
    return properties;
  }

  @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
  @Override
  protected void createImportableTable(String namespace, String table) throws Exception {
    testUtils.execute(
        "CREATE TABLE "
            + rdbEngine.encloseFullTableName(namespace, table)
            + "("
            + rdbEngine.enclose("pk")
            + " CHAR(8),"
            + rdbEngine.enclose("col")
            + " CHAR(8), PRIMARY KEY("
            + rdbEngine.enclose("pk")
            + "))");
  }

  @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
  @Override
  protected void createNonImportableTable(String namespace, String table) throws Exception {
    testUtils.execute(
        "CREATE TABLE "
            + rdbEngine.encloseFullTableName(namespace, table)
            + "("
            + rdbEngine.enclose("pk")
            + " CHAR(8),"
            + rdbEngine.enclose("col")
            + " DATE, PRIMARY KEY("
            + rdbEngine.enclose("pk")
            + "))");
  }

  @Override
  protected void dropNonImportableTable(String namespace, String table) throws Exception {
    testUtils.dropTable(namespace, table);
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void importTables_ImportableTablesGiven_ShouldImportProperly() throws Exception {
    super.importTables_ImportableTablesGiven_ShouldImportProperly();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void importTables_ImportableTablesAndNonRelatedSameNameTableGiven_ShouldImportProperly()
      throws Exception {
    super.importTables_ImportableTablesAndNonRelatedSameNameTableGiven_ShouldImportProperly();
  }

  @Override
  public void afterAll() {
    try {
      super.afterAll();
    } catch (Exception e) {
      logger.warn("Failed to call super.afterAll", e);
    }

    try {
      if (testUtils != null) {
        testUtils.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close test utils", e);
    }
  }

  @SuppressWarnings("unused")
  private boolean isSqlite() {
    return JdbcEnv.isSqlite();
  }

  @Override
  protected void waitForDifferentSessionDdl() {
    if (JdbcTestUtils.isYugabyte(rdbEngine)) {
      // This is needed to avoid schema or catalog version mismatch database errors.
      Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
      return;
    }
    super.waitForDifferentSessionDdl();
  }
}
