package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.service.TransactionFactory;
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

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DistributedTransactionAdminRepairTableIntegrationTestBase {

  protected static final String TEST_NAME = "tx_admin_repair_table";
  protected static final String NAMESPACE = "int_test_" + TEST_NAME;

  protected static final String TABLE = "test_table";
  protected static final String COL_NAME1 = "c1";
  protected static final String COL_NAME2 = "c2";
  protected static final String COL_NAME3 = "c3";
  protected static final String COL_NAME4 = "c4";
  protected static final String COL_NAME5 = "c5";
  protected static final String COL_NAME6 = "c6";
  protected static final String COL_NAME7 = "c7";
  protected static final String COL_NAME8 = "c8";
  protected static final String COL_NAME9 = "c9";
  protected static final String COL_NAME10 = "c10";
  protected static final String COL_NAME11 = "c11";

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

  protected DistributedTransactionAdmin admin;
  protected AdminTestUtils adminTestUtils;

  @BeforeAll
  public void beforeAll() throws Exception {
    initialize(TEST_NAME);
  }

  protected void initialize(String testName) throws Exception {}

  protected abstract Properties getProperties(String testName);

  protected void waitForDifferentSessionDdl() {
    // No wait by default.
  }

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
    admin.createCoordinatorTables(true, options);
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  private void dropTable() throws ExecutionException {
    admin.dropTable(getNamespace(), TABLE);
    admin.dropNamespace(getNamespace());
    admin.dropCoordinatorTables(true);
  }

  @BeforeEach
  protected void setUp() throws Exception {
    TransactionFactory factory = TransactionFactory.create(getProperties(TEST_NAME));
    admin = factory.getTransactionAdmin();
    createTable();
    adminTestUtils = getAdminTestUtils(TEST_NAME);
  }

  protected abstract AdminTestUtils getAdminTestUtils(String testName);

  @AfterEach
  protected void afterEach() throws Exception {
    dropTable();
    admin.close();
  }

  @AfterAll
  protected void afterAll() throws Exception {}

  @Test
  public void repairTableAndCoordinatorTable_ForDeletedMetadataTable_ShouldRepairProperly()
      throws Exception {
    // Arrange
    adminTestUtils.dropMetadataTable();

    // Act
    waitForDifferentSessionDdl();
    admin.repairTable(getNamespace(), getTable(), TABLE_METADATA, getCreationOptions());
    admin.repairCoordinatorTables(getCreationOptions());

    // Assert
    waitForDifferentSessionDdl();
    assertThat(admin.tableExists(getNamespace(), TABLE)).isTrue();
    assertThat(admin.getTableMetadata(getNamespace(), TABLE)).isEqualTo(TABLE_METADATA);
    assertThat(admin.coordinatorTablesExist()).isTrue();
    if (hasCoordinatorTables()) {
      assertThat(adminTestUtils.areTableMetadataForCoordinatorTablesPresent()).isTrue();
    }
  }

  @Test
  public void repairTableAndCoordinatorTable_ForTruncatedMetadataTable_ShouldRepairProperly()
      throws Exception {
    // Arrange
    adminTestUtils.truncateMetadataTable();

    // Act
    admin.repairTable(getNamespace(), getTable(), TABLE_METADATA, getCreationOptions());
    admin.repairCoordinatorTables(getCreationOptions());

    // Assert
    assertThat(admin.tableExists(getNamespace(), TABLE)).isTrue();
    assertThat(admin.getTableMetadata(getNamespace(), TABLE)).isEqualTo(TABLE_METADATA);
    if (hasCoordinatorTables()) {
      assertThat(adminTestUtils.areTableMetadataForCoordinatorTablesPresent()).isTrue();
    }
  }

  @Test
  public void
      repairTableAndCoordinatorTable_CoordinatorTablesDoNotExist_ShouldThrowIllegalArgumentException()
          throws ExecutionException {
    if (!hasCoordinatorTables()) {
      return;
    }

    // Arrange
    admin.dropCoordinatorTables(true);

    // Act Assert
    waitForDifferentSessionDdl();
    assertThatThrownBy(() -> admin.repairCoordinatorTables(getCreationOptions()))
        .isInstanceOf(IllegalArgumentException.class);
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
    if (hasCoordinatorTables()) {
      assertThat(adminTestUtils.areTableMetadataForCoordinatorTablesPresent()).isTrue();
    }
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

  protected boolean hasCoordinatorTables() {
    return true;
  }
}
