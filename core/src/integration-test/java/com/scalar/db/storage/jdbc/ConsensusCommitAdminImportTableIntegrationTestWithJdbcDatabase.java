package com.scalar.db.storage.jdbc;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminImportTableIntegrationTestBase;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.condition.EnabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsensusCommitAdminImportTableIntegrationTestWithJdbcDatabase
    extends ConsensusCommitAdminImportTableIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(ConsensusCommitAdminImportTableIntegrationTestWithJdbcDatabase.class);

  private JdbcAdminImportTestUtils testUtils;

  @Override
  protected Properties getProps(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    testUtils = new JdbcAdminImportTestUtils(properties);
    return JdbcEnv.getProperties(testName);
  }

  @Override
  public void afterAll() {
    try {
      super.afterAll();
    } catch (Exception e) {
      logger.warn("Failed to call super.afterAll", e);
    }
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
