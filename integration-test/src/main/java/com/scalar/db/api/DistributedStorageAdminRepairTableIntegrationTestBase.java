package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.util.AdminTestUtils;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DistributedStorageAdminRepairTableIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(DistributedStorageAdminRepairTableIntegrationTestBase.class);

  private static final String TEST_NAME = "storage_admin_repair_table";
  private static final String NAMESPACE = "int_test_" + TEST_NAME;

  private static final String TABLE = "test_table";
  private static final String COL_NAME1 = "c1";
  private static final String COL_NAME2 = "c2";
  private static final String COL_NAME3 = "c3";
  private static final String COL_NAME4 = "c4";
  private static final String COL_NAME5 = "c5";
  private static final String COL_NAME6 = "c6";
  private static final String COL_NAME7 = "c7";
  private static final String COL_NAME8 = "c8";
  private static final String COL_NAME9 = "c9";
  private static final String COL_NAME10 = "c10";
  private static final String COL_NAME11 = "c11";

  protected static final TableMetadata TABLE_METADATA =
      TableMetadata.newBuilder()
          .addColumn(COL_NAME1, DataType.INT)
          .addColumn(COL_NAME2, DataType.TEXT)
          .addColumn(COL_NAME3, DataType.TEXT)
          .addColumn(COL_NAME4, DataType.INT)
          .addColumn(COL_NAME5, DataType.INT)
          .addColumn(COL_NAME6, DataType.TEXT)
          .addColumn(COL_NAME7, DataType.BIGINT)
          .addColumn(COL_NAME8, DataType.FLOAT)
          .addColumn(COL_NAME9, DataType.DOUBLE)
          .addColumn(COL_NAME10, DataType.BOOLEAN)
          .addColumn(COL_NAME11, DataType.BLOB)
          .addPartitionKey(COL_NAME2)
          .addPartitionKey(COL_NAME1)
          .addClusteringKey(COL_NAME4, Scan.Ordering.Order.ASC)
          .addClusteringKey(COL_NAME3, Scan.Ordering.Order.DESC)
          .addSecondaryIndex(COL_NAME5)
          .addSecondaryIndex(COL_NAME6)
          .build();

  protected DistributedStorageAdmin admin;

  protected AdminTestUtils adminTestUtils;

  @BeforeAll
  public void beforeAll() throws Exception {
    initialize(TEST_NAME);
  }

  protected void initialize(String testName) throws Exception {}

  @AfterAll
  public void afterAll() throws Exception {
    try {
      if (admin != null) {
        admin.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close admin", e);
    }
  }

  protected abstract Properties getProperties(String testName);

  protected String getNamespace() {
    return NAMESPACE;
  }

  protected String getTable() {
    return TABLE;
  }

  private void createTable() throws ExecutionException {
    Map<String, String> options = getCreationOptions();
    admin.createNamespace(getNamespace(), options);
    admin.createTable(getNamespace(), getTable(), TABLE_METADATA, options);
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  private void dropTable() throws ExecutionException {
    admin.dropTable(getNamespace(), getTable());
    admin.dropNamespace(getNamespace());
  }

  @BeforeEach
  protected void setUp() throws Exception {
    StorageFactory factory = StorageFactory.create(getProperties(TEST_NAME));
    admin = factory.getStorageAdmin();
    createTable();
    adminTestUtils = getAdminTestUtils(TEST_NAME);
  }

  protected abstract AdminTestUtils getAdminTestUtils(String testName);

  @AfterEach
  protected void afterEach() throws Exception {
    dropTable();
    admin.close();
  }

  protected void waitForDifferentSessionDdl() {
    // No wait by default.
  }

  @Test
  public void repairTable_ForDeletedMetadataTable_ShouldRepairProperly() throws Exception {
    // Arrange
    adminTestUtils.dropMetadataTable();

    // Act
    waitForDifferentSessionDdl();
    admin.repairTable(getNamespace(), getTable(), TABLE_METADATA, getCreationOptions());

    // Assert
    waitForDifferentSessionDdl();
    assertThat(admin.tableExists(getNamespace(), getTable())).isTrue();
    assertThat(admin.getTableMetadata(getNamespace(), getTable())).isEqualTo(TABLE_METADATA);
  }

  @Test
  public void repairTable_ForTruncatedMetadataTable_ShouldRepairProperly() throws Exception {
    // Arrange
    adminTestUtils.truncateMetadataTable();

    // Act
    admin.repairTable(getNamespace(), getTable(), TABLE_METADATA, getCreationOptions());

    // Assert
    assertThat(admin.tableExists(getNamespace(), getTable())).isTrue();
    assertThat(admin.getTableMetadata(getNamespace(), getTable())).isEqualTo(TABLE_METADATA);
  }

  @Test
  public void repairTable_ForCorruptedMetadataTable_ShouldRepairProperly() throws Exception {
    // Arrange
    adminTestUtils.corruptMetadata(getNamespace(), getTable());

    // Act
    admin.repairTable(getNamespace(), getTable(), TABLE_METADATA, getCreationOptions());

    // Assert
    assertThat(admin.tableExists(getNamespace(), getTable())).isTrue();
    assertThat(admin.getTableMetadata(getNamespace(), getTable())).isEqualTo(TABLE_METADATA);
  }

  @Test
  public void repairTable_ForNonExistingTable_ShouldThrowIllegalArgument() {
    // Arrange

    // Act Assert
    assertThatThrownBy(
            () ->
                admin.repairTable(
                    getNamespace(), "non-existing-table", TABLE_METADATA, getCreationOptions()))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
