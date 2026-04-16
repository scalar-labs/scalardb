package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.util.concurrent.Uninterruptibles;
import com.google.errorprone.annotations.concurrent.LazyInit;
import com.scalar.db.api.DistributedStorageAdminRepairIntegrationTestBase;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

public class JdbcAdminRepairIntegrationTest
    extends DistributedStorageAdminRepairIntegrationTestBase {

  private static final String RENAMED_TABLE = "tbl_renamed";
  private static final String COL_NAME16 = "c16";

  @LazyInit private RdbEngineStrategy rdbEngine;
  @LazyInit private JdbcAdminTestUtils jdbcAdminTestUtils;

  @Override
  protected Properties getProperties(String testName) {
    return JdbcEnv.getProperties(testName);
  }

  @Override
  protected void initialize(String testName) throws Exception {
    super.initialize(testName);
    Properties properties = getProperties(testName);
    jdbcAdminTestUtils = new JdbcAdminTestUtils(properties);
    adminTestUtils = jdbcAdminTestUtils;
    rdbEngine = RdbEngineFactory.create(new JdbcConfig(new DatabaseConfig(properties)));
  }

  @AfterEach
  @Override
  protected void afterEach() throws Exception {
    // Clean up renamed table if it exists (for renameTable tests)
    try {
      admin.dropTable(getNamespace(), RENAMED_TABLE, true);
    } catch (Exception ignored) {
    }
    try {
      jdbcAdminTestUtils.deleteMetadata(getNamespace(), RENAMED_TABLE);
    } catch (Exception ignored) {
    }
    // If the original table no longer exists at storage level (e.g., renamed or dropped),
    // clean up its orphaned metadata so super.afterEach() doesn't fail trying to drop it
    try {
      if (!adminTestUtils.tableExists(getNamespace(), getTable())) {
        jdbcAdminTestUtils.deleteMetadata(getNamespace(), getTable());
      }
    } catch (Exception ignored) {
    }
    super.afterEach();
  }

  @Override
  protected void waitForDifferentSessionDdl() {
    if (JdbcTestUtils.isYugabyte(rdbEngine)) {
      Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
      return;
    }
    super.waitForDifferentSessionDdl();
  }

  // createTable failure tests (1-1, 1-2, 1-3)

  @Test
  public void repairTable_AfterCreateTableFailedBeforeIndexCreation_ShouldCreateIndexesAndMetadata()
      throws Exception {
    // Arrange: Drop all indexes + delete metadata
    String c5IndexName = JdbcAdminTestUtils.getIndexName(getNamespace(), getTable(), COL_NAME5);
    String c6IndexName = JdbcAdminTestUtils.getIndexName(getNamespace(), getTable(), COL_NAME6);
    jdbcAdminTestUtils.dropIndex(getNamespace(), getTable(), c5IndexName);
    jdbcAdminTestUtils.dropIndex(getNamespace(), getTable(), c6IndexName);
    jdbcAdminTestUtils.deleteMetadata(getNamespace(), getTable());

    // Act
    waitForDifferentSessionDdl();
    admin.repairTable(getNamespace(), getTable(), getTableMetadata(), getCreationOptions());

    // Assert
    waitForDifferentSessionDdl();
    assertThat(admin.getTableMetadata(getNamespace(), getTable())).isEqualTo(getTableMetadata());
    assertThat(jdbcAdminTestUtils.indexExists(getNamespace(), getTable(), c5IndexName)).isTrue();
    assertThat(jdbcAdminTestUtils.indexExists(getNamespace(), getTable(), c6IndexName)).isTrue();
  }

  @Test
  public void
      repairTable_AfterCreateTableFailedDuringPartialIndexCreation_ShouldCreateRemainingIndexesAndMetadata()
          throws Exception {
    // Arrange: Drop only c6 index, keep c5 index
    String c6IndexName = JdbcAdminTestUtils.getIndexName(getNamespace(), getTable(), COL_NAME6);
    jdbcAdminTestUtils.dropIndex(getNamespace(), getTable(), c6IndexName);
    jdbcAdminTestUtils.deleteMetadata(getNamespace(), getTable());

    // Act
    waitForDifferentSessionDdl();
    admin.repairTable(getNamespace(), getTable(), getTableMetadata(), getCreationOptions());

    // Assert
    waitForDifferentSessionDdl();
    assertThat(admin.getTableMetadata(getNamespace(), getTable())).isEqualTo(getTableMetadata());
    String c5IndexName = JdbcAdminTestUtils.getIndexName(getNamespace(), getTable(), COL_NAME5);
    assertThat(jdbcAdminTestUtils.indexExists(getNamespace(), getTable(), c5IndexName)).isTrue();
    assertThat(jdbcAdminTestUtils.indexExists(getNamespace(), getTable(), c6IndexName)).isTrue();
  }

  @Test
  public void repairTable_AfterCreateTableFailedBeforeMetadataInsertion_ShouldCreateMetadata()
      throws Exception {
    // Arrange: Delete metadata only (table and indexes still exist)
    jdbcAdminTestUtils.deleteMetadata(getNamespace(), getTable());

    // Act
    admin.repairTable(getNamespace(), getTable(), getTableMetadata(), getCreationOptions());

    // Assert
    assertThat(admin.getTableMetadata(getNamespace(), getTable())).isEqualTo(getTableMetadata());
  }

  // createIndex failure tests (3-1, 3-2)

  @Test
  public void
      repairTable_AfterCreateIndexFailedBeforeIndexCreation_ShouldCreateIndexAndUpdateMetadata()
          throws Exception {
    // Arrange: Remove c5 index completely via admin (storage + metadata)
    admin.dropIndex(getNamespace(), getTable(), COL_NAME5);

    // Act: repairTable with original metadata (c5 indexed)
    waitForDifferentSessionDdl();
    admin.repairTable(getNamespace(), getTable(), getTableMetadata(), getCreationOptions());

    // Assert
    waitForDifferentSessionDdl();
    assertThat(admin.getTableMetadata(getNamespace(), getTable())).isEqualTo(getTableMetadata());
    String c5IndexName = JdbcAdminTestUtils.getIndexName(getNamespace(), getTable(), COL_NAME5);
    assertThat(jdbcAdminTestUtils.indexExists(getNamespace(), getTable(), c5IndexName)).isTrue();
  }

  @Test
  public void repairTable_AfterCreateIndexFailedBeforeMetadataUpdate_ShouldUpdateMetadata()
      throws Exception {
    // Arrange: Remove c5 index completely, then recreate at storage level only
    admin.dropIndex(getNamespace(), getTable(), COL_NAME5);
    waitForDifferentSessionDdl();
    String c5IndexName = JdbcAdminTestUtils.getIndexName(getNamespace(), getTable(), COL_NAME5);
    jdbcAdminTestUtils.createIndex(getNamespace(), getTable(), COL_NAME5, c5IndexName);

    // Act: repairTable with original metadata (c5 indexed)
    waitForDifferentSessionDdl();
    admin.repairTable(getNamespace(), getTable(), getTableMetadata(), getCreationOptions());

    // Assert
    waitForDifferentSessionDdl();
    assertThat(admin.getTableMetadata(getNamespace(), getTable())).isEqualTo(getTableMetadata());
    assertThat(jdbcAdminTestUtils.indexExists(getNamespace(), getTable(), c5IndexName)).isTrue();
  }

  // dropIndex failure tests (4-1, 4-2)

  @Test
  public void repairTable_AfterDropIndexFailed_WithMetadataWithoutIndex_ShouldUpdateMetadata()
      throws Exception {
    // Arrange: Drop c5 index from storage only (metadata still says c5 indexed)
    String c5IndexName = JdbcAdminTestUtils.getIndexName(getNamespace(), getTable(), COL_NAME5);
    jdbcAdminTestUtils.dropIndex(getNamespace(), getTable(), c5IndexName);

    // Build metadata without c5 index
    TableMetadata metadataWithoutC5Index =
        TableMetadata.newBuilder(getTableMetadata()).removeSecondaryIndex(COL_NAME5).build();

    // Act: repairTable with metadata without c5 index
    waitForDifferentSessionDdl();
    admin.repairTable(getNamespace(), getTable(), metadataWithoutC5Index, getCreationOptions());

    // Assert
    waitForDifferentSessionDdl();
    assertThat(admin.getTableMetadata(getNamespace(), getTable()))
        .isEqualTo(metadataWithoutC5Index);
    assertThat(jdbcAdminTestUtils.indexExists(getNamespace(), getTable(), c5IndexName)).isFalse();
  }

  @Test
  public void repairTable_AfterDropIndexFailed_WithMetadataWithIndex_ShouldRecreateIndex()
      throws Exception {
    // Arrange: Drop c5 index from storage only (metadata still says c5 indexed)
    String c5IndexName = JdbcAdminTestUtils.getIndexName(getNamespace(), getTable(), COL_NAME5);
    jdbcAdminTestUtils.dropIndex(getNamespace(), getTable(), c5IndexName);

    // Act: repairTable with original metadata (c5 indexed)
    waitForDifferentSessionDdl();
    admin.repairTable(getNamespace(), getTable(), getTableMetadata(), getCreationOptions());

    // Assert
    waitForDifferentSessionDdl();
    assertThat(admin.getTableMetadata(getNamespace(), getTable())).isEqualTo(getTableMetadata());
    assertThat(jdbcAdminTestUtils.indexExists(getNamespace(), getTable(), c5IndexName)).isTrue();
  }

  // addNewColumnToTable failure test (5-1)

  @Test
  public void repairTable_AfterAddColumnFailedBeforeMetadataUpdate_ShouldUpdateMetadata()
      throws Exception {
    // Arrange: Add c16 INT column to storage directly
    String sqlType = rdbEngine.getDataTypeForEngine(DataType.INT);
    jdbcAdminTestUtils.addColumn(getNamespace(), getTable(), COL_NAME16, sqlType);

    // Build metadata with c16
    TableMetadata metadataWithC16 =
        TableMetadata.newBuilder(getTableMetadata()).addColumn(COL_NAME16, DataType.INT).build();

    // Act
    admin.repairTable(getNamespace(), getTable(), metadataWithC16, getCreationOptions());

    // Assert
    assertThat(admin.getTableMetadata(getNamespace(), getTable())).isEqualTo(metadataWithC16);
  }

  // dropColumnFromTable failure test (6-1)

  @Test
  public void repairTable_AfterDropColumnFailedBeforeMetadataUpdate_ShouldUpdateMetadata()
      throws Exception {
    // Arrange: Drop c7 (BIGINT, non-key, non-index) from storage
    jdbcAdminTestUtils.dropColumn(getNamespace(), getTable(), COL_NAME7);

    // Build metadata without c7
    TableMetadata metadataWithoutC7 =
        TableMetadata.newBuilder(getTableMetadata()).removeColumn(COL_NAME7).build();

    // Act
    waitForDifferentSessionDdl();
    admin.repairTable(getNamespace(), getTable(), metadataWithoutC7, getCreationOptions());

    // Assert
    waitForDifferentSessionDdl();
    assertThat(admin.getTableMetadata(getNamespace(), getTable())).isEqualTo(metadataWithoutC7);
  }

  // renameColumn failure tests (7-1, 7-2)

  @Test
  public void
      repairTable_AfterRenameColumnFailedBeforeIndexRename_ShouldCreateNewIndexAndUpdateMetadata()
          throws Exception {
    Assumptions.assumeFalse(JdbcTestUtils.isSqlite(rdbEngine));

    // Arrange: Rename c5 to c5_new at storage level (index still has old name)
    String sqlType = rdbEngine.getDataTypeForEngine(DataType.INT);
    jdbcAdminTestUtils.renameColumn(getNamespace(), getTable(), COL_NAME5, "c5_new", sqlType);

    // Build metadata with c5_new indexed
    TableMetadata metadataWithC5New =
        TableMetadata.newBuilder(getTableMetadata())
            .renameColumn(COL_NAME5, "c5_new")
            .renameSecondaryIndex(COL_NAME5, "c5_new")
            .build();

    // Act
    waitForDifferentSessionDdl();
    admin.repairTable(getNamespace(), getTable(), metadataWithC5New, getCreationOptions());

    // Assert
    waitForDifferentSessionDdl();
    assertThat(admin.getTableMetadata(getNamespace(), getTable())).isEqualTo(metadataWithC5New);
    String newIndexName = JdbcAdminTestUtils.getIndexName(getNamespace(), getTable(), "c5_new");
    assertThat(jdbcAdminTestUtils.indexExists(getNamespace(), getTable(), newIndexName)).isTrue();
  }

  @Test
  public void repairTable_AfterRenameColumnFailedBeforeMetadataUpdate_ShouldUpdateMetadata()
      throws Exception {
    Assumptions.assumeFalse(JdbcTestUtils.isSqlite(rdbEngine));

    // Arrange: Rename c5 to c5_new at storage level + rename index
    String sqlType = rdbEngine.getDataTypeForEngine(DataType.INT);
    jdbcAdminTestUtils.renameColumn(getNamespace(), getTable(), COL_NAME5, "c5_new", sqlType);
    jdbcAdminTestUtils.renameIndex(getNamespace(), getTable(), COL_NAME5, getTable(), "c5_new");

    // Build metadata with c5_new indexed
    TableMetadata metadataWithC5New =
        TableMetadata.newBuilder(getTableMetadata())
            .renameColumn(COL_NAME5, "c5_new")
            .renameSecondaryIndex(COL_NAME5, "c5_new")
            .build();

    // Act
    waitForDifferentSessionDdl();
    admin.repairTable(getNamespace(), getTable(), metadataWithC5New, getCreationOptions());

    // Assert
    waitForDifferentSessionDdl();
    assertThat(admin.getTableMetadata(getNamespace(), getTable())).isEqualTo(metadataWithC5New);
    String newIndexName = JdbcAdminTestUtils.getIndexName(getNamespace(), getTable(), "c5_new");
    assertThat(jdbcAdminTestUtils.indexExists(getNamespace(), getTable(), newIndexName)).isTrue();
  }

  // alterColumnType failure test (8-1)

  @Test
  public void repairTable_AfterAlterColumnTypeFailedBeforeMetadataUpdate_ShouldUpdateMetadata()
      throws Exception {
    // Arrange: Alter c7 from BIGINT to FLOAT at storage level
    String sqlType = rdbEngine.getDataTypeForEngine(DataType.FLOAT);
    jdbcAdminTestUtils.alterColumnType(getNamespace(), getTable(), COL_NAME7, sqlType);

    // Build metadata with c7 as FLOAT
    TableMetadata metadataWithC7Float =
        TableMetadata.newBuilder(getTableMetadata())
            .removeColumn(COL_NAME7)
            .addColumn(COL_NAME7, DataType.FLOAT)
            .build();

    // Act
    admin.repairTable(getNamespace(), getTable(), metadataWithC7Float, getCreationOptions());

    // Assert
    assertThat(admin.getTableMetadata(getNamespace(), getTable())).isEqualTo(metadataWithC7Float);
  }

  // renameTable failure tests (9-1, 9-2, 9-3)

  @Test
  public void repairTable_AfterRenameTableFailedBeforeIndexRename_ShouldCreateMetadataAndIndexes()
      throws Exception {
    // Arrange: Rename table at storage level (all indexes still have old names)
    jdbcAdminTestUtils.renameTable(getNamespace(), getTable(), RENAMED_TABLE);
    jdbcAdminTestUtils.deleteMetadata(getNamespace(), getTable());

    // Act
    waitForDifferentSessionDdl();
    admin.repairTable(getNamespace(), RENAMED_TABLE, getTableMetadata(), getCreationOptions());

    // Assert
    waitForDifferentSessionDdl();
    assertThat(admin.getTableMetadata(getNamespace(), RENAMED_TABLE)).isEqualTo(getTableMetadata());
    String c5NewIndexName =
        JdbcAdminTestUtils.getIndexName(getNamespace(), RENAMED_TABLE, COL_NAME5);
    String c6NewIndexName =
        JdbcAdminTestUtils.getIndexName(getNamespace(), RENAMED_TABLE, COL_NAME6);
    assertThat(jdbcAdminTestUtils.indexExists(getNamespace(), RENAMED_TABLE, c5NewIndexName))
        .isTrue();
    assertThat(jdbcAdminTestUtils.indexExists(getNamespace(), RENAMED_TABLE, c6NewIndexName))
        .isTrue();
  }

  @Test
  public void
      repairTable_AfterRenameTableFailedDuringPartialIndexRename_ShouldCreateMetadataAndMissingIndexes()
          throws Exception {
    // Arrange: Rename table + rename c5 index only (c6 index still has old name)
    jdbcAdminTestUtils.renameTable(getNamespace(), getTable(), RENAMED_TABLE);
    jdbcAdminTestUtils.renameIndex(getNamespace(), getTable(), COL_NAME5, RENAMED_TABLE, COL_NAME5);
    jdbcAdminTestUtils.deleteMetadata(getNamespace(), getTable());

    // Act
    waitForDifferentSessionDdl();
    admin.repairTable(getNamespace(), RENAMED_TABLE, getTableMetadata(), getCreationOptions());

    // Assert
    waitForDifferentSessionDdl();
    assertThat(admin.getTableMetadata(getNamespace(), RENAMED_TABLE)).isEqualTo(getTableMetadata());
    String c5NewIndexName =
        JdbcAdminTestUtils.getIndexName(getNamespace(), RENAMED_TABLE, COL_NAME5);
    String c6NewIndexName =
        JdbcAdminTestUtils.getIndexName(getNamespace(), RENAMED_TABLE, COL_NAME6);
    assertThat(jdbcAdminTestUtils.indexExists(getNamespace(), RENAMED_TABLE, c5NewIndexName))
        .isTrue();
    assertThat(jdbcAdminTestUtils.indexExists(getNamespace(), RENAMED_TABLE, c6NewIndexName))
        .isTrue();
  }

  @Test
  public void repairTable_AfterRenameTableFailedAfterIndexRename_ShouldCreateMetadata()
      throws Exception {
    // Arrange: Rename table + rename all indexes
    jdbcAdminTestUtils.renameTable(getNamespace(), getTable(), RENAMED_TABLE);
    jdbcAdminTestUtils.renameIndex(getNamespace(), getTable(), COL_NAME5, RENAMED_TABLE, COL_NAME5);
    jdbcAdminTestUtils.renameIndex(getNamespace(), getTable(), COL_NAME6, RENAMED_TABLE, COL_NAME6);
    jdbcAdminTestUtils.deleteMetadata(getNamespace(), getTable());

    // Act
    waitForDifferentSessionDdl();
    admin.repairTable(getNamespace(), RENAMED_TABLE, getTableMetadata(), getCreationOptions());

    // Assert
    waitForDifferentSessionDdl();
    assertThat(admin.getTableMetadata(getNamespace(), RENAMED_TABLE)).isEqualTo(getTableMetadata());
    String c5NewIndexName =
        JdbcAdminTestUtils.getIndexName(getNamespace(), RENAMED_TABLE, COL_NAME5);
    String c6NewIndexName =
        JdbcAdminTestUtils.getIndexName(getNamespace(), RENAMED_TABLE, COL_NAME6);
    assertThat(jdbcAdminTestUtils.indexExists(getNamespace(), RENAMED_TABLE, c5NewIndexName))
        .isTrue();
    assertThat(jdbcAdminTestUtils.indexExists(getNamespace(), RENAMED_TABLE, c6NewIndexName))
        .isTrue();
  }

  // Namespace failure tests (11-1, 12-1)

  @Test
  public void
      repairNamespace_AfterCreateNamespaceFailedBeforeMetadataInsertion_ShouldCreateMetadata()
          throws Exception {
    // Arrange: Delete namespace metadata only (schema and table still exist)
    jdbcAdminTestUtils.deleteNamespaceMetadata(getNamespace());

    // Act
    admin.repairNamespace(getNamespace(), getCreationOptions());

    // Assert
    assertThat(adminTestUtils.namespaceExists(getNamespace())).isTrue();
    assertThat(admin.namespaceExists(getNamespace())).isTrue();
  }

  @Test
  public void repairNamespace_AfterDropNamespaceFailedWithOrphanedMetadata_ShouldRecreateNamespace()
      throws Exception {
    // Arrange: Drop table and schema at storage level (metadata remains)
    adminTestUtils.dropTable(getNamespace(), getTable());
    waitForDifferentSessionDdl();
    adminTestUtils.dropNamespace(getNamespace());

    // Act
    waitForDifferentSessionDdl();
    admin.repairNamespace(getNamespace(), getCreationOptions());

    // Assert
    waitForDifferentSessionDdl();
    assertThat(adminTestUtils.namespaceExists(getNamespace())).isTrue();
    assertThat(admin.namespaceExists(getNamespace())).isTrue();
  }

  // Tests for repairTable with before/after state (A→B model)

  @Test
  public void repairTable_AfterRenameTable_ShouldCleanUpOldTableMetadata() throws Exception {
    // Arrange: Rename table at storage level (old metadata remains)
    jdbcAdminTestUtils.renameTable(getNamespace(), getTable(), RENAMED_TABLE);

    // Act: repairTable with before/after state
    waitForDifferentSessionDdl();
    admin.repairTable(
        getNamespace(),
        getTable(),
        getTableMetadata(),
        RENAMED_TABLE,
        getTableMetadata(),
        getCreationOptions());

    // Assert: old table metadata should be cleaned up
    waitForDifferentSessionDdl();
    assertThat(admin.getTableMetadata(getNamespace(), getTable())).isNull();
  }

  @Test
  public void repairTable_AfterRenameTable_ShouldDropOldNameIndexes() throws Exception {
    // Arrange: Rename table at storage level (indexes still have old names)
    jdbcAdminTestUtils.renameTable(getNamespace(), getTable(), RENAMED_TABLE);

    // Act
    waitForDifferentSessionDdl();
    admin.repairTable(
        getNamespace(),
        getTable(),
        getTableMetadata(),
        RENAMED_TABLE,
        getTableMetadata(),
        getCreationOptions());

    // Assert: old-name indexes should not exist on the renamed table
    waitForDifferentSessionDdl();
    String oldC5IndexName = JdbcAdminTestUtils.getIndexName(getNamespace(), getTable(), COL_NAME5);
    String oldC6IndexName = JdbcAdminTestUtils.getIndexName(getNamespace(), getTable(), COL_NAME6);
    assertThat(jdbcAdminTestUtils.indexExists(getNamespace(), RENAMED_TABLE, oldC5IndexName))
        .isFalse();
    assertThat(jdbcAdminTestUtils.indexExists(getNamespace(), RENAMED_TABLE, oldC6IndexName))
        .isFalse();
  }

  @Test
  public void repairTable_AfterRenameColumn_ShouldDropOldNameIndex() throws Exception {
    Assumptions.assumeFalse(JdbcTestUtils.isSqlite(rdbEngine));

    // Arrange: Rename c5 to c5_new at storage level (index still has old name)
    String sqlType = rdbEngine.getDataTypeForEngine(DataType.INT);
    jdbcAdminTestUtils.renameColumn(getNamespace(), getTable(), COL_NAME5, "c5_new", sqlType);

    TableMetadata metadataWithC5New =
        TableMetadata.newBuilder(getTableMetadata())
            .renameColumn(COL_NAME5, "c5_new")
            .renameSecondaryIndex(COL_NAME5, "c5_new")
            .build();

    // Act
    waitForDifferentSessionDdl();
    admin.repairTable(
        getNamespace(),
        getTable(),
        getTableMetadata(),
        getTable(),
        metadataWithC5New,
        getCreationOptions());

    // Assert: old-name index should not exist
    waitForDifferentSessionDdl();
    String oldIndexName = JdbcAdminTestUtils.getIndexName(getNamespace(), getTable(), COL_NAME5);
    assertThat(jdbcAdminTestUtils.indexExists(getNamespace(), getTable(), oldIndexName)).isFalse();
  }

  @Test
  public void repairTable_AfterDropColumn_ShouldDropColumnFromStorage() throws Exception {
    // Arrange: Drop index on c5, but keep the c5 column in storage
    String c5IndexName = JdbcAdminTestUtils.getIndexName(getNamespace(), getTable(), COL_NAME5);
    jdbcAdminTestUtils.dropIndex(getNamespace(), getTable(), c5IndexName);

    // Build metadata without c5
    TableMetadata metadataWithoutC5 =
        TableMetadata.newBuilder(getTableMetadata())
            .removeColumn(COL_NAME5)
            .removeSecondaryIndex(COL_NAME5)
            .build();

    // Act
    waitForDifferentSessionDdl();
    admin.repairTable(
        getNamespace(),
        getTable(),
        getTableMetadata(),
        getTable(),
        metadataWithoutC5,
        getCreationOptions());

    // Assert: c5 should not exist in storage
    waitForDifferentSessionDdl();
    assertThat(jdbcAdminTestUtils.columnExists(getNamespace(), getTable(), COL_NAME5)).isFalse();
  }
}
