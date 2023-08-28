package com.scalar.db.transaction.jdbc;

import com.scalar.db.api.DistributedTransactionAdminImportTableIntegrationTestBase;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.jdbc.JdbcAdminImportTestUtils;
import com.scalar.db.storage.jdbc.JdbcEnv;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.condition.EnabledIf;

public class JdbcTransactionAdminImportTableIntegrationTest
    extends DistributedTransactionAdminImportTableIntegrationTestBase {

  private JdbcAdminImportTestUtils testUtils;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = new Properties();
    properties.putAll(JdbcEnv.getProperties(testName));
    properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "jdbc");
    testUtils = new JdbcAdminImportTestUtils(properties);
    return properties;
  }

  @Override
  protected Map<String, TableMetadata> createExistingDatabaseWithAllDataTypes()
      throws SQLException {
    return testUtils.createExistingDatabaseWithAllDataTypes(getNamespace());
  }

  @Override
  protected void dropNonImportableTable(String table) throws SQLException {
    testUtils.dropTable(getNamespace(), table);
  }

  @SuppressWarnings("unused")
  private boolean isSqlite() {
    return JdbcEnv.isSqlite();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void importTable_ShouldWorkProperly() throws Exception {
    super.importTable_ShouldWorkProperly();
  }

  @Test
  @Override
  @EnabledIf("isSqlite")
  public void importTable_ForUnsupportedDatabase_ShouldThrowUnsupportedOperationException()
      throws ExecutionException {
    super.importTable_ForUnsupportedDatabase_ShouldThrowUnsupportedOperationException();
  }
}
