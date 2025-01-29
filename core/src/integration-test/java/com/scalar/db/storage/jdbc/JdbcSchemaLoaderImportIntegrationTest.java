package com.scalar.db.storage.jdbc;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.schemaloader.SchemaLoaderImportIntegrationTestBase;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.Map;
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
    String sql;

    if (JdbcTestUtils.isMysql(rdbEngine)) {
      sql =
          "CREATE TABLE "
              + rdbEngine.encloseFullTableName(namespace, table)
              + "("
              + rdbEngine.enclose("pk")
              + " CHAR(8),"
              + rdbEngine.enclose("col1")
              + " CHAR(8),"
              + rdbEngine.enclose("col2")
              + " DATETIME,"
              + "PRIMARY KEY("
              + rdbEngine.enclose("pk")
              + "))";
    } else if (JdbcTestUtils.isOracle(rdbEngine)) {
      sql =
          "CREATE TABLE "
              + rdbEngine.encloseFullTableName(namespace, table)
              + "("
              + rdbEngine.enclose("pk")
              + " CHAR(8),"
              + rdbEngine.enclose("col1")
              + " CHAR(8),"
              + rdbEngine.enclose("col2")
              + " DATE,"
              + "PRIMARY KEY("
              + rdbEngine.enclose("pk")
              + "))";
    } else if (JdbcTestUtils.isPostgresql(rdbEngine) || JdbcTestUtils.isSqlServer(rdbEngine)) {
      sql =
          "CREATE TABLE "
              + rdbEngine.encloseFullTableName(namespace, table)
              + "("
              + rdbEngine.enclose("pk")
              + " CHAR(8),"
              + rdbEngine.enclose("col1")
              + " CHAR(8),"
              + "PRIMARY KEY("
              + rdbEngine.enclose("pk")
              + "))";
    } else {
      throw new AssertionError();
    }

    testUtils.execute(sql);
  }

  @Override
  protected Map<String, DataType> getImportableTableOverrideColumnsType() {
    // col1 type override confirms overriding with the default data type mapping does not fail
    // col2 really performs a type override
    if (JdbcTestUtils.isMysql(rdbEngine)) {
      return ImmutableMap.of("col1", DataType.TEXT, "col2", DataType.TIMESTAMPTZ);
    } else if (JdbcTestUtils.isOracle(rdbEngine)) {
      return ImmutableMap.of("col1", DataType.TEXT, "col2", DataType.TIMESTAMP);
    } else if (JdbcTestUtils.isPostgresql(rdbEngine) || JdbcTestUtils.isSqlServer(rdbEngine)) {
      return ImmutableMap.of("col1", DataType.TEXT);
    } else if (JdbcTestUtils.isSqlite(rdbEngine)) {
      return Collections.emptyMap();
    } else {
      throw new AssertionError();
    }
  }

  @Override
  protected TableMetadata getImportableTableMetadata(boolean hasTypeOverride) {
    TableMetadata.Builder metadata = TableMetadata.newBuilder();
    metadata.addPartitionKey("pk");
    metadata.addColumn("pk", DataType.TEXT);
    metadata.addColumn("col1", DataType.TEXT);

    if (JdbcTestUtils.isMysql(rdbEngine)) {
      return metadata
          .addColumn("col2", hasTypeOverride ? DataType.TIMESTAMPTZ : DataType.TIMESTAMP)
          .build();
    } else if (JdbcTestUtils.isOracle(rdbEngine)) {
      return metadata
          .addColumn("col2", hasTypeOverride ? DataType.TIMESTAMP : DataType.DATE)
          .build();
    } else if (JdbcTestUtils.isPostgresql(rdbEngine) || JdbcTestUtils.isSqlServer(rdbEngine)) {
      return metadata.build();
    } else {
      throw new AssertionError();
    }
  }

  @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
  @Override
  protected void createNonImportableTable(String namespace, String table) throws Exception {
    String nonImportableDataType;
    if (JdbcTestUtils.isMysql(rdbEngine)) {
      nonImportableDataType = "YEAR";
    } else if (JdbcTestUtils.isPostgresql(rdbEngine)) {
      nonImportableDataType = "INTERVAL";
    } else if (JdbcTestUtils.isOracle(rdbEngine)) {
      nonImportableDataType = "INT";
    } else if (JdbcTestUtils.isSqlServer(rdbEngine)) {
      nonImportableDataType = "MONEY";
    } else if (JdbcTestUtils.isSqlite(rdbEngine)) {
      nonImportableDataType = "TEXT";
    } else {
      throw new AssertionError();
    }
    testUtils.execute(
        "CREATE TABLE "
            + rdbEngine.encloseFullTableName(namespace, table)
            + "("
            + rdbEngine.enclose("pk")
            + " CHAR(8),"
            + rdbEngine.enclose("col")
            + " "
            + nonImportableDataType
            + ", PRIMARY KEY("
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
