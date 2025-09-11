package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThatCode;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminIntegrationTestBase;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.condition.EnabledIf;

public class ConsensusCommitAdminIntegrationTestWithJdbcDatabase
    extends ConsensusCommitAdminIntegrationTestBase {

  private RdbEngineStrategy rdbEngine;

  @Override
  protected Properties getProps(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    rdbEngine = RdbEngineFactory.create(new JdbcConfig(new DatabaseConfig(properties)));
    return properties;
  }

  @Override
  protected String getSystemNamespaceName(Properties properties) {
    return new JdbcConfig(new DatabaseConfig(properties))
        .getTableMetadataSchema()
        .orElse(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
  }

  @Override
  protected String getCoordinatorNamespaceName(String testName) {
    return new ConsensusCommitConfig(new DatabaseConfig(getProperties(testName)))
        .getCoordinatorNamespace()
        .orElse(Coordinator.NAMESPACE);
  }

  @Override
  protected boolean isGroupCommitEnabled(String testName) {
    return new ConsensusCommitConfig(new DatabaseConfig(getProperties(testName)))
        .isCoordinatorGroupCommitEnabled();
  }

  // Since SQLite doesn't have persistent namespaces, some behaviors around the namespace are
  // different from the other adapters. So disable several tests that check such behaviors.

  @SuppressWarnings("unused")
  private boolean isSqlite() {
    return JdbcEnv.isSqlite();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void createNamespace_ForNonExistingNamespace_ShouldCreateNamespaceProperly()
      throws ExecutionException {
    super.createNamespace_ForNonExistingNamespace_ShouldCreateNamespaceProperly();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void createNamespace_ForExistingNamespace_ShouldThrowIllegalArgumentException() {
    super.createNamespace_ForExistingNamespace_ShouldThrowIllegalArgumentException();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void createNamespace_IfNotExists_ForExistingNamespace_ShouldNotThrowAnyException() {
    super.createNamespace_IfNotExists_ForExistingNamespace_ShouldNotThrowAnyException();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void dropNamespace_ForNonExistingNamespace_ShouldDropNamespaceProperly()
      throws ExecutionException {
    super.dropNamespace_ForNonExistingNamespace_ShouldDropNamespaceProperly();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void dropNamespace_ForNonExistingNamespace_ShouldThrowIllegalArgumentException() {
    super.dropNamespace_ForNonExistingNamespace_ShouldThrowIllegalArgumentException();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void dropNamespace_ForNonEmptyNamespace_ShouldThrowIllegalArgumentException()
      throws ExecutionException {
    super.dropNamespace_ForNonEmptyNamespace_ShouldThrowIllegalArgumentException();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void dropNamespace_IfExists_ForNonExistingNamespace_ShouldNotThrowAnyException() {
    super.dropNamespace_IfExists_ForNonExistingNamespace_ShouldNotThrowAnyException();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void namespaceExists_ShouldReturnCorrectResults() throws ExecutionException {
    super.namespaceExists_ShouldReturnCorrectResults();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void createTable_ForNonExistingNamespace_ShouldThrowIllegalArgumentException() {
    super.createTable_ForNonExistingNamespace_ShouldThrowIllegalArgumentException();
  }

  @Override
  protected boolean isCreateIndexOnTextAndBlobColumnsEnabled() {
    // "admin.createIndex()" for TEXT and BLOB columns fails (the "create index" query runs
    // indefinitely) on the Db2 community edition docker version which we use for the CI.
    // However, the index creation is successful on Db2 hosted on IBM Cloud.
    // So we disable these tests until the issue with the Db2 community edition is resolved.
    return !JdbcTestUtils.isDb2(rdbEngine);
  }

  @SuppressWarnings("unused")
  private boolean isDb2() {
    return JdbcEnv.isDb2();
  }

  @Test
  @Override
  @DisabledIf("isDb2")
  public void renameColumn_ForPrimaryKeyColumn_ShouldRenameColumnCorrectly()
      throws ExecutionException {
    super.renameColumn_ForPrimaryKeyColumn_ShouldRenameColumnCorrectly();
  }

  @Test
  @Override
  @DisabledIf("isDb2")
  public void renameColumn_ForIndexKeyColumn_ShouldRenameColumnAndIndexCorrectly()
      throws ExecutionException {
    super.renameColumn_ForIndexKeyColumn_ShouldRenameColumnAndIndexCorrectly();
  }

  @Test
  @EnabledIf("isDb2")
  public void renameColumn_Db2_ForPrimaryOrIndexKeyColumn_ShouldThrowUnsupportedOperationException()
      throws ExecutionException {
    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata currentTableMetadata =
          TableMetadata.newBuilder()
              .addColumn(COL_NAME1, DataType.INT)
              .addColumn(COL_NAME2, DataType.INT)
              .addColumn(COL_NAME3, DataType.TEXT)
              .addPartitionKey(COL_NAME1)
              .addClusteringKey(COL_NAME2)
              .addSecondaryIndex(COL_NAME3)
              .build();
      admin.createTable(namespace1, TABLE4, currentTableMetadata, options);

      // Act Assert
      assertThatCode(() -> admin.renameColumn(namespace1, TABLE4, COL_NAME1, COL_NAME4))
          .isInstanceOf(UnsupportedOperationException.class);
      assertThatCode(() -> admin.renameColumn(namespace1, TABLE4, COL_NAME2, COL_NAME4))
          .isInstanceOf(UnsupportedOperationException.class);
      assertThatCode(() -> admin.renameColumn(namespace1, TABLE4, COL_NAME3, COL_NAME4))
          .isInstanceOf(UnsupportedOperationException.class);
    } finally {
      admin.dropTable(namespace1, TABLE4, true);
    }
  }
}
