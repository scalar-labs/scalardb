package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageAdminImportTableIntegrationTestBase;
import com.scalar.db.api.TableMetadata;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

public class JdbcAdminImportTableIntegrationTest
    extends DistributedStorageAdminImportTableIntegrationTestBase {

  private JdbcAdminImportTestUtils testUtils;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    testUtils = new JdbcAdminImportTestUtils(properties);
    return JdbcEnv.getProperties(testName);
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
  @Disabled("Disable this test until the admin.repairNamespace() API is created")
  public void importTable_ShouldWorkProperly() throws Exception {
    super.importTable_ShouldWorkProperly();
  }

  @Test
  @Override
  @EnabledIf("isSqlite")
  public void importTable_ForUnsupportedDatabase_ShouldThrowUnsupportedOperationException() {
    super.importTable_ForUnsupportedDatabase_ShouldThrowUnsupportedOperationException();
  }
}
