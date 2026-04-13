package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.scalar.db.api.DistributedStorageAdminIntegrationTestBase;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.util.AdminTestUtils;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

public class JdbcAdminIntegrationTest extends DistributedStorageAdminIntegrationTestBase {
  private RdbEngineStrategy rdbEngine;

  @Override
  protected Properties getProperties(String testName) {
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
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new JdbcAdminTestUtils(getProperties(testName));
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
  public void
      dropNamespace_ForNamespaceWithNonScalarDBManagedTables_ShouldThrowIllegalArgumentException() {}

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
    // indefinitely) on Db2 community edition version but works on Db2 hosted on IBM Cloud.
    // So we disable these tests until the issue is resolved.
    return !JdbcTestUtils.isDb2(rdbEngine);
  }

  @SuppressWarnings("unused")
  private boolean isDb2() {
    return JdbcTestUtils.isDb2(rdbEngine);
  }

  @Test
  @DisabledIf("isDb2")
  public void dropIndex_WithLongIndexNameCreatedByOldNaming_ShouldDropIndexByFallback()
      throws Exception {
    // Use a long column name that causes the index name to exceed the max length
    // The column name is chosen so that the original index name
    // (index_{namespace}_{table}_{column}) is exactly 64 characters, which exceeds the 63-character
    // limit to trigger shortening but is still within MySQL's 64-character limit.
    String longColumn = "long_column_name_for_testing1";
    JdbcAdminTestUtils testUtils = (JdbcAdminTestUtils) getAdminTestUtils(getTestName());
    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata tableMetadata =
          TableMetadata.newBuilder()
              .addColumn(getColumnName1(), DataType.INT)
              .addColumn(longColumn, DataType.INT)
              .addPartitionKey(getColumnName1())
              .addSecondaryIndex(longColumn)
              .build();
      admin.createTable(getNamespace1(), getTable4(), tableMetadata, options);

      // Replace the shortened index with the old (long) naming convention
      String shortenedIndexName = JdbcAdmin.getIndexName(getNamespace1(), getTable4(), longColumn);
      String originalIndexName =
          String.join("_", "index", getNamespace1(), getTable4(), longColumn);
      assertThat(originalIndexName.length()).isEqualTo(JdbcUtils.MAX_INDEX_NAME_LENGTH + 1);
      testUtils.dropIndex(getNamespace1(), getTable4(), shortenedIndexName);
      testUtils.createIndex(getNamespace1(), getTable4(), longColumn, originalIndexName);

      // Act Assert - dropIndex should succeed via fallback
      assertThatCode(() -> admin.dropIndex(getNamespace1(), getTable4(), longColumn))
          .doesNotThrowAnyException();
      assertThat(admin.indexExists(getNamespace1(), getTable4(), longColumn)).isFalse();
    } finally {
      admin.dropTable(getNamespace1(), getTable4(), true);
      testUtils.close();
    }
  }
}
