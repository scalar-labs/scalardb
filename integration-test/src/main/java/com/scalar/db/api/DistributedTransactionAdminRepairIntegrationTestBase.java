package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.service.TransactionFactory;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
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
public abstract class DistributedTransactionAdminRepairIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(DistributedTransactionAdminRepairIntegrationTestBase.class);

  protected static final String TEST_NAME = "tx_admin_repair";
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
  protected DistributedStorageAdmin storageAdmin;
  protected AdminTestUtils adminTestUtils = null;

  @BeforeAll
  public void beforeAll() throws Exception {
    initialize(TEST_NAME);
  }

  protected void initialize(String testName) throws Exception {
    TransactionFactory factory = TransactionFactory.create(getProperties(TEST_NAME));
    admin = factory.getTransactionAdmin();

    StorageFactory storageFactory = StorageFactory.create(getProperties(TEST_NAME));
    storageAdmin = storageFactory.getStorageAdmin();
  }

  @AfterAll
  public void afterAll() throws Exception {
    try {
      if (admin != null) {
        admin.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close admin", e);
    }

    try {
      if (storageAdmin != null) {
        storageAdmin.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close storage admin", e);
    }

    try {
      if (adminTestUtils != null) {
        adminTestUtils.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close admin test utils", e);
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
    admin.createCoordinatorTables(options);
    admin.createNamespace(getNamespace(), options);
    admin.createTable(getNamespace(), getTable(), TABLE_METADATA, options);
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  private void dropTable() throws ExecutionException {
    admin.dropTable(getNamespace(), getTable(), true);
    admin.dropNamespace(getNamespace(), true);
    admin.dropCoordinatorTables(true);
  }

  @BeforeEach
  protected void setUp() throws Exception {
    createTable();
  }

  @AfterEach
  protected void afterEach() throws Exception {
    dropTable();
  }

  @Test
  public void repairTable_ForExistingTableAndMetadata_ShouldDoNothing() throws Exception {
    // Act
    admin.repairTable(getNamespace(), getTable(), TABLE_METADATA, getCreationOptions());

    // Assert
    assertThat(adminTestUtils.tableExists(getNamespace(), getTable())).isTrue();
    assertThat(admin.getTableMetadata(getNamespace(), getTable())).isEqualTo(TABLE_METADATA);
  }

  @Test
  public void repairTableAndCoordinatorTables_ForDeletedMetadataTable_ShouldRepairProperly()
      throws Exception {
    // Arrange
    adminTestUtils.dropMetadataTable();

    // Act
    admin.repairTable(getNamespace(), getTable(), TABLE_METADATA, getCreationOptions());
    admin.repairCoordinatorTables(getCreationOptions());

    // Assert
    assertThat(admin.tableExists(getNamespace(), TABLE)).isTrue();
    assertThat(admin.getTableMetadata(getNamespace(), TABLE)).isEqualTo(TABLE_METADATA);
    assertThat(adminTestUtils.areTableAndMetadataForCoordinatorTablesPresent()).isTrue();
  }

  @Test
  public void repairTableAndCoordinatorTables_ForTruncatedMetadataTable_ShouldRepairProperly()
      throws Exception {
    // Arrange
    adminTestUtils.truncateMetadataTable();

    // Act
    admin.repairTable(getNamespace(), getTable(), TABLE_METADATA, getCreationOptions());
    admin.repairCoordinatorTables(getCreationOptions());

    // Assert
    assertThat(admin.tableExists(getNamespace(), TABLE)).isTrue();
    assertThat(admin.getTableMetadata(getNamespace(), TABLE)).isEqualTo(TABLE_METADATA);
    assertThat(adminTestUtils.areTableAndMetadataForCoordinatorTablesPresent()).isTrue();
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
    assertThat(adminTestUtils.areTableAndMetadataForCoordinatorTablesPresent()).isTrue();
  }

  @Test
  public void repairCoordinatorTables_CoordinatorTablesExist_ShouldDoNothing() throws Exception {
    // Act
    admin.repairCoordinatorTables(getCreationOptions());

    // Assert
    assertThat(adminTestUtils.areTableAndMetadataForCoordinatorTablesPresent()).isTrue();
  }

  @Test
  public void repairCoordinatorTables_CoordinatorTablesDoNotExist_ShouldCreateCoordinatorTables()
      throws Exception {
    // Arrange
    admin.dropCoordinatorTables();

    // Act
    admin.repairCoordinatorTables(getCreationOptions());

    // Assert
    assertThat(adminTestUtils.areTableAndMetadataForCoordinatorTablesPresent()).isTrue();
  }

  @Test
  public void repairTable_ForNonExistingTableButExistingMetadata_ShouldCreateTable()
      throws Exception {
    // Arrange
    adminTestUtils.dropTable(getNamespace(), getTable());

    // Act
    admin.repairTable(getNamespace(), getTable(), TABLE_METADATA, getCreationOptions());

    // Assert
    assertThat(adminTestUtils.tableExists(getNamespace(), getTable())).isTrue();
    assertThat(admin.getTableMetadata(getNamespace(), getTable())).isEqualTo(TABLE_METADATA);
    assertThat(adminTestUtils.areTableAndMetadataForCoordinatorTablesPresent()).isTrue();
  }

  @Test
  public void repairNamespace_ForExistingNamespace_ShouldDoNothing() throws Exception {
    // Act
    admin.repairNamespace(getNamespace(), getCreationOptions());

    // Assert
    assertThat(adminTestUtils.namespaceExists(getNamespace())).isTrue();
    assertThat(admin.namespaceExists(getNamespace())).isTrue();
  }

  @Test
  public void
      repairNamespaceAndCoordinatorTables_ForExistingNamespaceButDeletedNamespacesTable_ShouldCreateMetadata()
          throws Exception {
    // Arrange
    adminTestUtils.dropNamespacesTable();

    // Act
    admin.repairNamespace(getNamespace(), getCreationOptions());
    admin.repairCoordinatorTables(getCreationOptions());

    // Assert
    assertThat(adminTestUtils.namespaceExists(getNamespace())).isTrue();
    assertThat(admin.namespaceExists(getNamespace())).isTrue();
    assertThat(coordinatorNamespaceMetadataExits()).isTrue();
  }

  @Test
  public void repairNamespaceAndCoordinatorTables_ForTruncatedNamespacesTable_ShouldRepairProperly()
      throws Exception {
    // Arrange
    adminTestUtils.truncateNamespacesTable();

    // Act
    admin.repairNamespace(getNamespace(), getCreationOptions());
    admin.repairCoordinatorTables(getCreationOptions());

    // Assert
    assertThat(adminTestUtils.namespaceExists(getNamespace())).isTrue();
    assertThat(admin.namespaceExists(getNamespace())).isTrue();
    assertThat(coordinatorNamespaceMetadataExits()).isTrue();
  }

  @Test
  public void repairNamespace_ForNonExistingNamespaceButExistingMetadata_ShouldCreateNamespace()
      throws Exception {
    // Arrange
    admin.dropTable(getNamespace(), getTable());
    adminTestUtils.dropNamespace(getNamespace());

    // Act
    admin.repairNamespace(getNamespace(), getCreationOptions());

    // Assert
    assertThat(adminTestUtils.namespaceExists(getNamespace())).isTrue();
    assertThat(admin.namespaceExists(getNamespace())).isTrue();
  }

  private boolean coordinatorNamespaceMetadataExits() throws ExecutionException {
    String coordinatorNamespace =
        new ConsensusCommitConfig(new DatabaseConfig(getProperties(TEST_NAME)))
            .getCoordinatorNamespace()
            .orElse(Coordinator.NAMESPACE);
    return storageAdmin.namespaceExists(coordinatorNamespace);
  }
}
