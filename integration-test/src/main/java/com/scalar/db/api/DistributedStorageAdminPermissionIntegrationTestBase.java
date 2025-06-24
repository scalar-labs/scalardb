package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThatCode;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.util.AdminTestUtils;
import com.scalar.db.util.PermissionTestUtils;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DistributedStorageAdminPermissionIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(DistributedStorageAdminPermissionIntegrationTestBase.class);

  private static final String TEST_NAME = "storage_admin";
  private static final String NAMESPACE1 = "test_" + TEST_NAME + "_1";
  private static final String NAMESPACE2 = "test_" + TEST_NAME + "_2";
  private static final String TABLE1 = "test_table_1";
  private static final String TABLE2 = "test_table_2";
  private static final String COL_NAME1 = "c1";
  private static final String COL_NAME2 = "c2";
  private static final String COL_NAME3 = "c3";
  private static final String COL_NAME4 = "c4";
  private static final String RAW_COL_NAME = "raw_col";
  private static final String NEW_COL_NAME = "new_col";
  private static final TableMetadata TABLE_METADATA =
      TableMetadata.newBuilder()
          .addColumn(COL_NAME1, DataType.INT)
          .addColumn(COL_NAME2, DataType.TEXT)
          .addColumn(COL_NAME3, DataType.TEXT)
          .addColumn(COL_NAME4, DataType.INT)
          .addPartitionKey(COL_NAME1)
          .addClusteringKey(COL_NAME2, Scan.Ordering.Order.ASC)
          .addSecondaryIndex(COL_NAME4)
          .build();
  private DistributedStorageAdmin adminForRootUser;
  private DistributedStorageAdmin adminForNormalUser;
  private String normalUserName;

  @BeforeAll
  public void beforeAll() throws Exception {
    Properties propertiesForRootUser = getProperties(TEST_NAME);
    Properties propertiesForNormalUser = getPropertiesForNormalUser(TEST_NAME);

    // Initialize the admin for root user
    StorageFactory factoryForRootUser = StorageFactory.create(propertiesForRootUser);
    adminForRootUser = factoryForRootUser.getStorageAdmin();

    DatabaseConfig config = new DatabaseConfig(propertiesForNormalUser);
    if (!config.getUsername().isPresent() || !config.getPassword().isPresent()) {
      throw new IllegalArgumentException(
          "Username and password must be set in the properties for normal user");
    }
    // Create normal user and grant permissions
    PermissionTestUtils permissionTestUtils = getPermissionTestUtils(TEST_NAME);
    normalUserName = config.getUsername().get();
    permissionTestUtils.createNormalUser(normalUserName, config.getPassword().get());
    permissionTestUtils.grantRequiredPermission(normalUserName);
    permissionTestUtils.close();

    // Initialize the admin for normal user
    StorageFactory factoryForNormalUser = StorageFactory.create(propertiesForNormalUser);
    adminForNormalUser = factoryForNormalUser.getStorageAdmin();
    
    // Create the namespace and table
    adminForRootUser.createNamespace(NAMESPACE1, true, getCreationOptions());
    adminForRootUser.createTable(NAMESPACE1, TABLE1, TABLE_METADATA, true, getCreationOptions());
  }

  @AfterAll
  public void afterAll() throws Exception {
    try {
      // Drop the table and namespace created for the tests
      adminForRootUser.dropTable(NAMESPACE1, TABLE1, true);
      adminForRootUser.dropTable(NAMESPACE1, TABLE2, true);
      adminForRootUser.dropNamespace(NAMESPACE1, true);
      adminForRootUser.dropNamespace(NAMESPACE2, true);
      // Close the admin instances
      adminForRootUser.close();
      adminForNormalUser.close();
      // Drop normal user
      PermissionTestUtils permissionTestUtils = getPermissionTestUtils(TEST_NAME);
      permissionTestUtils.dropNormalUser(normalUserName);
      permissionTestUtils.close();
    } catch (Exception e) {
      logger.warn("Failed to close admin", e);
    }
  }

  @Test
  public void getImportTableMetadata_WithSufficientPermission_ShouldSucceed() {
    // Arrange
    // Act Assert
    assertThatCode(() -> adminForNormalUser.getImportTableMetadata(NAMESPACE1, TABLE1))
        .doesNotThrowAnyException();
  }

  @Test
  public void addRawColumnToTable_WithSufficientPermission_ShouldSucceed() {
    // Arrange
    // Act Assert
    assertThatCode(
            () ->
                adminForNormalUser.addRawColumnToTable(
                    NAMESPACE1, TABLE1, RAW_COL_NAME, DataType.INT))
        .doesNotThrowAnyException();
  }

  @Test
  public void createNamespace_WithSufficientPermission_ShouldSucceed() {
    // Arrange
    // Act Assert
    assertThatCode(() -> adminForNormalUser.createNamespace(NAMESPACE2, getCreationOptions()))
        .doesNotThrowAnyException();
  }

  @Test
  public void createTable_WithSufficientPermission_ShouldSucceed() throws ExecutionException {
    try {
      // Arrange
      // Act Assert
      assertThatCode(
              () ->
                  adminForNormalUser.createTable(
                      NAMESPACE1, TABLE2, TABLE_METADATA, getCreationOptions()))
          .doesNotThrowAnyException();
    } finally {
      // Clean up
      adminForRootUser.dropTable(NAMESPACE1, TABLE2, true);
    }
  }

  @Test
  public void dropTable_WithSufficientPermission_ShouldSucceed() throws ExecutionException {
    try {
      // Arrange
      adminForRootUser.createTable(NAMESPACE1, TABLE2, TABLE_METADATA, true, getCreationOptions());
      // Act Assert
      assertThatCode(() -> adminForNormalUser.dropTable(NAMESPACE1, TABLE2, true))
          .doesNotThrowAnyException();
    } finally {
      // Clean up
      adminForRootUser.dropTable(NAMESPACE1, TABLE2, true);
    }
  }

  @Test
  public void dropNamespace_WithSufficientPermission_ShouldSucceed() throws ExecutionException {
    try {
      // Arrange
      adminForRootUser.createNamespace(NAMESPACE2, true, getCreationOptions());
      // Act Assert
      assertThatCode(() -> adminForNormalUser.dropNamespace(NAMESPACE2, true))
          .doesNotThrowAnyException();
    } finally {
      // Clean up
      adminForRootUser.dropNamespace(NAMESPACE2, true);
    }
  }

  @Test
  public void truncateTable_WithSufficientPermission_ShouldSucceed() {
    // Arrange
    // Act Assert
    assertThatCode(() -> adminForNormalUser.truncateTable(NAMESPACE1, TABLE1))
        .doesNotThrowAnyException();
  }

  @Test
  public void createIndex_WithSufficientPermission_ShouldSucceed() {
    // Arrange
    // Act Assert
    assertThatCode(
            () ->
                adminForNormalUser.createIndex(NAMESPACE1, TABLE1, COL_NAME3, getCreationOptions()))
        .doesNotThrowAnyException();
  }

  @Test
  public void dropIndex_WithSufficientPermission_ShouldSucceed() {
    // Arrange
    // Act Assert
    assertThatCode(() -> adminForNormalUser.dropIndex(NAMESPACE1, TABLE1, COL_NAME4))
        .doesNotThrowAnyException();
  }

  @Test
  public void indexExists_WithSufficientPermission_ShouldSucceed() {
    // Arrange
    // Act Assert
    assertThatCode(() -> adminForNormalUser.indexExists(NAMESPACE1, TABLE1, COL_NAME3))
        .doesNotThrowAnyException();
  }

  @Test
  public void getTableMetadata_WithSufficientPermission_ShouldSucceed() {
    // Arrange
    // Act Assert
    assertThatCode(() -> adminForNormalUser.getTableMetadata(NAMESPACE1, TABLE1))
        .doesNotThrowAnyException();
  }

  @Test
  public void getNamespaceTableNames_WithSufficientPermission_ShouldSucceed() {
    // Arrange
    // Act Assert
    assertThatCode(() -> adminForNormalUser.getNamespaceTableNames(NAMESPACE1))
        .doesNotThrowAnyException();
  }

  @Test
  public void namespaceExists_WithSufficientPermission_ShouldSucceed() {
    // Arrange
    // Act Assert
    assertThatCode(() -> adminForNormalUser.namespaceExists(NAMESPACE1)).doesNotThrowAnyException();
  }

  @Test
  public void tableExists_WithSufficientPermission_ShouldSucceed() {
    // Arrange
    // Act Assert
    assertThatCode(() -> adminForNormalUser.tableExists(NAMESPACE1, TABLE1))
        .doesNotThrowAnyException();
  }

  @Test
  public void repairNamespace_WithSufficientPermission_ShouldSucceed() {
    // Arrange
    // Act Assert
    assertThatCode(() -> adminForNormalUser.repairNamespace(NAMESPACE1, getCreationOptions()))
        .doesNotThrowAnyException();
  }

  @Test
  public void repairTable_WithSufficientPermission_ShouldSucceed() {
    // Arrange
    // Act Assert
    assertThatCode(
            () ->
                adminForNormalUser.repairTable(
                    NAMESPACE1, TABLE1, TABLE_METADATA, getCreationOptions()))
        .doesNotThrowAnyException();
  }

  @Test
  public void addNewColumnToTable_WithSufficientPermission_ShouldSucceed() {
    // Arrange
    // Act Assert
    assertThatCode(
            () ->
                adminForNormalUser.addNewColumnToTable(
                    NAMESPACE1, TABLE1, NEW_COL_NAME, DataType.INT))
        .doesNotThrowAnyException();
  }

  @Test
  public void importTable_WithSufficientPermission_ShouldSucceed() throws Exception {
    AdminTestUtils adminTestUtils = getAdminTestUtils(TEST_NAME);
    try {
      // Arrange
      adminTestUtils.dropNamespacesTable();
      adminTestUtils.dropMetadataTable();
      // Act Assert
      assertThatCode(() -> adminForNormalUser.importTable(NAMESPACE1, TABLE1, getCreationOptions()))
          .doesNotThrowAnyException();
    } finally {
      // Clean up
      adminForRootUser.dropTable(NAMESPACE1, TABLE1, true);
      adminForRootUser.createTable(NAMESPACE1, TABLE1, TABLE_METADATA, true, getCreationOptions());
      adminTestUtils.close();
    }
  }

  @Test
  public void getNamespaceNames_WithSufficientPermission_ShouldSucceed() {
    // Arrange
    // Act Assert
    assertThatCode(() -> adminForNormalUser.getNamespaceNames()).doesNotThrowAnyException();
  }

  @Test
  public void upgrade_WithSufficientPermission_ShouldSucceed() throws Exception {
    AdminTestUtils adminTestUtils = getAdminTestUtils(TEST_NAME);
    try {
      // Arrange
      adminTestUtils.dropNamespacesTable();
      // Act Assert
      assertThatCode(() -> adminForNormalUser.upgrade(getCreationOptions()))
          .doesNotThrowAnyException();
    } finally {
      adminTestUtils.close();
    }
  }

  protected abstract Properties getProperties(String testName);

  protected abstract Properties getPropertiesForNormalUser(String testName);

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  protected abstract AdminTestUtils getAdminTestUtils(String testName);

  protected abstract PermissionTestUtils getPermissionTestUtils(String testName);
}
