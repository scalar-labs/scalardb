package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.scalar.db.api.DistributedStorageAdminImportTableIntegrationTestBase.TestData;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Result;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminImportTableWithMetadataDecouplingIntegrationTestBase;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.condition.EnabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsensusCommitAdminImportTableWithMetadataDecouplingIntegrationTestWithJdbcDatabase
    extends ConsensusCommitAdminImportTableWithMetadataDecouplingIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(
          ConsensusCommitAdminImportTableWithMetadataDecouplingIntegrationTestWithJdbcDatabase
              .class);

  private static final int MAX_INSERT_ATTEMPTS = 3;

  private JdbcAdminImportTestUtils testUtils;

  @Override
  protected Properties getProps(String testName) {
    Properties properties = ConsensusCommitJdbcEnv.getProperties(testName);

    // Set the isolation level for consistency reads for virtual tables
    RdbEngineStrategy rdbEngine =
        RdbEngineFactory.create(new JdbcConfig(new DatabaseConfig(properties)));
    properties.setProperty(
        JdbcConfig.ISOLATION_LEVEL,
        JdbcTestUtils.getIsolationLevel(
                rdbEngine.getMinimumIsolationLevelForConsistentVirtualTableRead())
            .name());

    if (testUtils == null) {
      testUtils = new JdbcAdminImportTestUtils(properties);
    }
    return properties;
  }

  @AfterAll
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

  @Test
  @DisabledIf("isSqlite")
  public void importTable_WithDecouplingAndNonBinaryTextKey_ShouldReadAndWriteThroughVirtualTable()
      throws Exception {
    // Arrange
    String table = "non_binary_key_tbl";
    String importedCollation = testUtils.getNonBinaryTextKeyCollation();
    assumeTrue(importedCollation != null, "The engine has no per-column collation");
    testUtils.createTableWithTextKey(namespace, table, importedCollation);
    JdbcAdminTestUtils adminTestUtils = new JdbcAdminTestUtils(getProps(getTestName()));
    String virtualTable = getImportedTableName(table);
    try {
      // Act
      admin.importTable(namespace, table, getCreationOptions());
      insertWithRetry(
          Insert.newBuilder()
              .namespace(namespace)
              .table(virtualTable)
              .partitionKey(Key.ofText("pk", "k1"))
              .bigIntValue("col", 7L)
              .build());
      Optional<Result> result;
      DistributedTransaction tx = manager.start();
      try {
        result =
            tx.get(
                Get.newBuilder()
                    .namespace(namespace)
                    .table(virtualTable)
                    .partitionKey(Key.ofText("pk", "k1"))
                    .build());
        tx.commit();
      } catch (Exception e) {
        tx.abort();
        throw e;
      }

      // Assert
      assertThat(result).isPresent();
      assertThat(result.get().getBigInt("col")).isEqualTo(7L);
      assertThat(adminTestUtils.getColumnCollation(namespace, table + "_tx_metadata", "pk"))
          .isEqualTo(importedCollation);
    } finally {
      adminTestUtils.close();
      if (admin.tableExists(namespace, virtualTable)) {
        admin.dropTable(namespace, virtualTable);
      }
      // The base class cleanup fails after importTable_ShouldWorkProperly, whose test data it keeps
      admin.dropNamespace(namespace, true);
    }
  }

  // When using Oracle with the SERIALIZABLE isolation level, a CommitConflictException may occur
  // even without any conflicts, so the insert is retried in such a case.
  private void insertWithRetry(Insert insert) throws Exception {
    for (int attempt = 1; ; attempt++) {
      DistributedTransaction tx = manager.start();
      boolean committed = false;
      try {
        tx.insert(insert);
        tx.commit();
        committed = true;
        return;
      } catch (CommitConflictException e) {
        if (attempt >= MAX_INSERT_ATTEMPTS) {
          throw e;
        }
      } finally {
        if (!committed) {
          tx.abort();
        }
      }
    }
  }
}
