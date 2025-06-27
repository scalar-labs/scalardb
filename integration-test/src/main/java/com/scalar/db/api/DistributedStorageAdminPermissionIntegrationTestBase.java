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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DistributedStorageAdminPermissionIntegrationTestBase {

  protected static final String TEST_NAME = "storage_admin";
  protected static final String NAMESPACE = "test_" + TEST_NAME + "_1";
  protected static final String TABLE = "test_table_1";
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
  protected DistributedStorageAdmin adminForRootUser;
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
    normalUserName = config.getUsername().get();
    PermissionTestUtils permissionTestUtils = getPermissionTestUtils(TEST_NAME);
    try {
      permissionTestUtils.createNormalUser(normalUserName, config.getPassword().get());
      permissionTestUtils.grantRequiredPermission(normalUserName);
    } finally {
      permissionTestUtils.close();
    }

    // Initialize the admin for normal user
    StorageFactory factoryForNormalUser = StorageFactory.create(propertiesForNormalUser);
    adminForNormalUser = factoryForNormalUser.getStorageAdmin();
  }

  @AfterAll
  public void afterAll() throws Exception {
    // Drop the table and namespace created for the tests
    adminForRootUser.dropTable(NAMESPACE, TABLE, true);
    adminForRootUser.dropNamespace(NAMESPACE, true);
    // Close the admin instances
    adminForRootUser.close();
    adminForNormalUser.close();
    // Drop normal user
    PermissionTestUtils permissionTestUtils = getPermissionTestUtils(TEST_NAME);
    try {
      permissionTestUtils.dropNormalUser(normalUserName);
    } finally {
      permissionTestUtils.close();
    }
  }

  @BeforeEach
  public void beforeEach() throws ExecutionException {
    dropTableByRootIfExists();
    dropNamespaceByRootIfExists();
  }

  @AfterEach
  public void afterEach() {
    sleepBetweenTests();
  }

  @Test
  public void getImportTableMetadata_WithSufficientPermission_ShouldSucceed()
      throws ExecutionException {
    // Arrange
    createNamespaceByRoot();
    createTableByRoot();
    // Act Assert
    assertThatCode(() -> adminForNormalUser.getImportTableMetadata(NAMESPACE, TABLE))
        .doesNotThrowAnyException();
  }

  @Test
  public void addRawColumnToTable_WithSufficientPermission_ShouldSucceed()
      throws ExecutionException {
    // Arrange
    createNamespaceByRoot();
    createTableByRoot();
    // Act Assert
    assertThatCode(
            () ->
                adminForNormalUser.addRawColumnToTable(
                    NAMESPACE, TABLE, RAW_COL_NAME, DataType.INT))
        .doesNotThrowAnyException();
  }

  @Test
  public void createNamespace_WithSufficientPermission_ShouldSucceed() {
    // Arrange
    // Act Assert
    assertThatCode(() -> adminForNormalUser.createNamespace(NAMESPACE, getCreationOptions()))
        .doesNotThrowAnyException();
  }

  @Test
  public void createTable_WithSufficientPermission_ShouldSucceed() throws ExecutionException {
    // Arrange
    createNamespaceByRoot();
    // Act Assert
    assertThatCode(
            () ->
                adminForNormalUser.createTable(
                    NAMESPACE, TABLE, TABLE_METADATA, getCreationOptions()))
        .doesNotThrowAnyException();
  }

  @Test
  public void dropTable_WithSufficientPermission_ShouldSucceed() throws ExecutionException {
    // Arrange
    createNamespaceByRoot();
    createTableByRoot();
    // Act Assert
    assertThatCode(() -> adminForNormalUser.dropTable(NAMESPACE, TABLE)).doesNotThrowAnyException();
  }

  @Test
  public void dropNamespace_WithSufficientPermission_ShouldSucceed() throws ExecutionException {
    // Arrange
    createNamespaceByRoot();
    // Act Assert
    assertThatCode(() -> adminForNormalUser.dropNamespace(NAMESPACE, true))
        .doesNotThrowAnyException();
  }

  @Test
  public void truncateTable_WithSufficientPermission_ShouldSucceed() throws ExecutionException {
    // Arrange
    createNamespaceByRoot();
    createTableByRoot();
    // Act Assert
    assertThatCode(() -> adminForNormalUser.truncateTable(NAMESPACE, TABLE))
        .doesNotThrowAnyException();
  }

  @Test
  public void createIndex_WithSufficientPermission_ShouldSucceed() throws ExecutionException {
    // Arrange
    createNamespaceByRoot();
    createTableByRoot();
    // Act Assert
    assertThatCode(
            () -> adminForNormalUser.createIndex(NAMESPACE, TABLE, COL_NAME3, getCreationOptions()))
        .doesNotThrowAnyException();
  }

  @Test
  public void dropIndex_WithSufficientPermission_ShouldSucceed() throws ExecutionException {
    // Arrange
    createNamespaceByRoot();
    createTableByRoot();
    // Act Assert
    assertThatCode(() -> adminForNormalUser.dropIndex(NAMESPACE, TABLE, COL_NAME4))
        .doesNotThrowAnyException();
  }

  @Test
  public void indexExists_WithSufficientPermission_ShouldSucceed() throws ExecutionException {
    // Arrange
    createNamespaceByRoot();
    createTableByRoot();
    // Act Assert
    assertThatCode(() -> adminForNormalUser.indexExists(NAMESPACE, TABLE, COL_NAME4))
        .doesNotThrowAnyException();
  }

  @Test
  public void getTableMetadata_WithSufficientPermission_ShouldSucceed() throws ExecutionException {
    // Arrange
    createNamespaceByRoot();
    createTableByRoot();
    // Act Assert
    assertThatCode(() -> adminForNormalUser.getTableMetadata(NAMESPACE, TABLE))
        .doesNotThrowAnyException();
  }

  @Test
  public void getNamespaceTableNames_WithSufficientPermission_ShouldSucceed()
      throws ExecutionException {
    // Arrange
    createNamespaceByRoot();
    createTableByRoot();
    // Act Assert
    assertThatCode(() -> adminForNormalUser.getNamespaceTableNames(NAMESPACE))
        .doesNotThrowAnyException();
  }

  @Test
  public void namespaceExists_WithSufficientPermission_ShouldSucceed() throws ExecutionException {
    // Arrange
    createNamespaceByRoot();
    createTableByRoot();
    // Act Assert
    assertThatCode(() -> adminForNormalUser.namespaceExists(NAMESPACE)).doesNotThrowAnyException();
  }

  @Test
  public void tableExists_WithSufficientPermission_ShouldSucceed() throws ExecutionException {
    // Arrange
    createNamespaceByRoot();
    createTableByRoot();
    // Act Assert
    assertThatCode(() -> adminForNormalUser.tableExists(NAMESPACE, TABLE))
        .doesNotThrowAnyException();
  }

  @Test
  public void repairNamespace_WithSufficientPermission_ShouldSucceed() throws Exception {
    // Arrange
    createNamespaceByRoot();
    // Drop the namespaces table to simulate a repair scenario
    AdminTestUtils adminTestUtils = getAdminTestUtils(TEST_NAME);
    try {
      adminTestUtils.dropNamespacesTable();
    } finally {
      adminTestUtils.close();
    }
    // Act Assert
    assertThatCode(() -> adminForNormalUser.repairNamespace(NAMESPACE, getCreationOptions()))
        .doesNotThrowAnyException();
  }

  @Test
  public void repairTable_WithSufficientPermission_ShouldSucceed() throws Exception {
    // Arrange
    createNamespaceByRoot();
    createTableByRoot();
    // Drop the metadata table to simulate a repair scenario
    AdminTestUtils adminTestUtils = getAdminTestUtils(TEST_NAME);
    try {
      adminTestUtils.dropMetadataTable();
    } finally {
      adminTestUtils.close();
    }
    // Act Assert
    assertThatCode(
            () ->
                adminForNormalUser.repairTable(
                    NAMESPACE, TABLE, TABLE_METADATA, getCreationOptions()))
        .doesNotThrowAnyException();
  }

  @Test
  public void addNewColumnToTable_WithSufficientPermission_ShouldSucceed()
      throws ExecutionException {
    // Arrange
    createNamespaceByRoot();
    createTableByRoot();
    // Act Assert
    assertThatCode(
            () ->
                adminForNormalUser.addNewColumnToTable(
                    NAMESPACE, TABLE, NEW_COL_NAME, DataType.INT))
        .doesNotThrowAnyException();
  }

  @Test
  public void importTable_WithSufficientPermission_ShouldSucceed() throws Exception {
    // Arrange
    createNamespaceByRoot();
    createTableByRoot();
    AdminTestUtils adminTestUtils = getAdminTestUtils(TEST_NAME);
    try {
      adminTestUtils.dropNamespacesTable();
      adminTestUtils.dropMetadataTable();
    } finally {
      adminTestUtils.close();
    }
    // Act Assert
    assertThatCode(() -> adminForNormalUser.importTable(NAMESPACE, TABLE, getCreationOptions()))
        .doesNotThrowAnyException();
  }

  @Test
  public void getNamespaceNames_WithSufficientPermission_ShouldSucceed() throws ExecutionException {
    // Arrange
    createNamespaceByRoot();
    // Act Assert
    assertThatCode(() -> adminForNormalUser.getNamespaceNames()).doesNotThrowAnyException();
  }

  @Test
  public void upgrade_WithSufficientPermission_ShouldSucceed() throws Exception {
    // Arrange
    createNamespaceByRoot();
    createTableByRoot();
    AdminTestUtils adminTestUtils = getAdminTestUtils(TEST_NAME);
    try {
      adminTestUtils.dropNamespacesTable();
    } finally {
      adminTestUtils.close();
    }
    // Act Assert
    assertThatCode(() -> adminForNormalUser.upgrade(getCreationOptions()))
        .doesNotThrowAnyException();
  }

  protected abstract Properties getProperties(String testName);

  protected abstract Properties getPropertiesForNormalUser(String testName);

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  protected abstract AdminTestUtils getAdminTestUtils(String testName);

  protected abstract PermissionTestUtils getPermissionTestUtils(String testName);

  protected void waitForTableCreation() {}

  protected void waitForNamespaceCreation() {}

  protected void waitForTableDeletion() {}

  protected void waitForNamespaceDeletion() {}

  protected void sleepBetweenTests() {}

  private void createNamespaceByRoot() throws ExecutionException {
    adminForRootUser.createNamespace(NAMESPACE, getCreationOptions());
    waitForNamespaceCreation();
  }

  private void createTableByRoot() throws ExecutionException {
    adminForRootUser.createTable(NAMESPACE, TABLE, TABLE_METADATA, getCreationOptions());
    waitForTableCreation();
  }

  private void dropNamespaceByRootIfExists() throws ExecutionException {
    adminForRootUser.dropNamespace(NAMESPACE, true);
    waitForNamespaceDeletion();
  }

  private void dropTableByRootIfExists() throws ExecutionException {
    adminForRootUser.dropTable(NAMESPACE, TABLE, true);
    waitForTableDeletion();
  }
}
