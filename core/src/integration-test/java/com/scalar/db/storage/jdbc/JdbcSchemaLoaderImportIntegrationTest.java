package com.scalar.db.storage.jdbc;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderImportIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

public class JdbcSchemaLoaderImportIntegrationTest extends SchemaLoaderImportIntegrationTestBase {

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

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new JdbcAdminTestUtils(getProperties(testName));
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

  @SuppressWarnings("unused")
  private boolean isSqlite() {
    return JdbcEnv.isSqlite();
  }
}
