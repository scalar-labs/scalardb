package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageAdminImportTableIntegrationTestBase.TestData;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminImportTableWithMetadataDecouplingIntegrationTestBase;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.condition.EnabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@DisabledIf("com.scalar.db.storage.jdbc.JdbcEnv#isOracle")
public class ConsensusCommitAdminImportTableWithMetadataDecouplingIntegrationTestWithJdbcDatabase
    extends ConsensusCommitAdminImportTableWithMetadataDecouplingIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(
          ConsensusCommitAdminImportTableWithMetadataDecouplingIntegrationTestWithJdbcDatabase
              .class);

  private JdbcAdminImportTestUtils testUtils;

  @Override
  protected Properties getProps(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);

    // Set the isolation level for consistency reads for virtual tables
    RdbEngineStrategy rdbEngine =
        RdbEngineFactory.create(new JdbcConfig(new DatabaseConfig(properties)));
    properties.setProperty(
        JdbcConfig.ISOLATION_LEVEL,
        JdbcTestUtils.getIsolationLevel(
                rdbEngine.getMinimumIsolationLevelForConsistentVirtualTableRead())
            .name());

    testUtils = new JdbcAdminImportTestUtils(properties);
    return properties;
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

  @Override
  protected List<TestData> createExistingDatabaseWithAllDataTypes() throws SQLException {
    return testUtils.createExistingDatabaseWithAllDataTypes(namespace);
  }

  @Override
  protected void dropNonImportableTable(String table) throws SQLException {
    testUtils.dropTable(namespace, table);
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
