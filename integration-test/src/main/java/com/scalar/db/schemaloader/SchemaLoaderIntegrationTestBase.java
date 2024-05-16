package com.scalar.db.schemaloader;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.service.TransactionFactory;
import com.scalar.db.util.AdminTestUtils;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class SchemaLoaderIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(SchemaLoaderIntegrationTestBase.class);
  private static final String TEST_NAME = "schema_loader";
  private static final Path CONFIG_FILE_PATH = Paths.get("config.properties").toAbsolutePath();
  private static final Path SCHEMA_FILE_PATH = Paths.get("schema.json").toAbsolutePath();
  private static final Path ALTERED_SCHEMA_FILE_PATH =
      Paths.get("altered_schema.json").toAbsolutePath();

  private static final String NAMESPACE_1 = "int_test_" + TEST_NAME + "1";
  private static final String TABLE_1 = "test_table1";
  private static final String NAMESPACE_2 = "int_test_" + TEST_NAME + "2";
  private static final String TABLE_2 = "test_table2";
  private static final TableMetadata TABLE_1_METADATA =
      TableMetadata.newBuilder()
          .addPartitionKey("pk1")
          .addClusteringKey("ck1", Order.DESC)
          .addClusteringKey("ck2", Order.ASC)
          .addColumn("pk1", DataType.INT)
          .addColumn("ck1", DataType.INT)
          .addColumn("ck2", DataType.TEXT)
          .addColumn("col1", DataType.INT)
          .addColumn("col2", DataType.BIGINT)
          .addColumn("col3", DataType.FLOAT)
          .addColumn("col4", DataType.DOUBLE)
          .addColumn("col5", DataType.TEXT)
          .addColumn("col6", DataType.BLOB)
          .addColumn("col7", DataType.BOOLEAN)
          .addSecondaryIndex("col1")
          .addSecondaryIndex("col5")
          .build();
  private static final TableMetadata TABLE_2_METADATA =
      TableMetadata.newBuilder()
          .addPartitionKey("pk1")
          .addClusteringKey("ck1", Order.ASC)
          .addColumn("pk1", DataType.INT)
          .addColumn("ck1", DataType.INT)
          .addColumn("col1", DataType.INT)
          .addColumn("col2", DataType.BIGINT)
          .addColumn("col3", DataType.FLOAT)
          .build();

  private DistributedStorageAdmin storageAdmin;
  private DistributedTransactionAdmin transactionAdmin;
  private String namespace1;
  private String namespace2;
  private String systemNamespaceName;
  private AdminTestUtils adminTestUtils;

  @BeforeAll
  public void beforeAll() throws Exception {
    initialize(TEST_NAME);
    Properties properties = getProperties(TEST_NAME);
    namespace1 = getNamespace1();
    namespace2 = getNamespace2();
    writeConfigFile(properties);
    writeSchemaFile(SCHEMA_FILE_PATH, getSchemaJsonMap());
    writeSchemaFile(ALTERED_SCHEMA_FILE_PATH, getAlteredSchemaJsonMap());
    StorageFactory factory = StorageFactory.create(properties);
    storageAdmin = factory.getStorageAdmin();
    TransactionFactory transactionFactory = TransactionFactory.create(properties);
    transactionAdmin = transactionFactory.getTransactionAdmin();
    systemNamespaceName = new DatabaseConfig(properties).getSystemNamespaceName();
    adminTestUtils = getAdminTestUtils(TEST_NAME);
  }

  @BeforeEach
  public void setUp() throws Exception {
    dropTablesIfExist();
  }

  protected void initialize(String testName) throws Exception {}

  protected abstract Properties getProperties(String testName);

  protected void writeConfigFile(Properties properties) throws IOException {
    try (OutputStream outputStream = Files.newOutputStream(CONFIG_FILE_PATH)) {
      properties.store(outputStream, null);
    }
  }

  protected String getNamespace1() {
    return NAMESPACE_1;
  }

  protected String getNamespace2() {
    return NAMESPACE_2;
  }

  protected abstract AdminTestUtils getAdminTestUtils(String testName);

  protected Map<String, Object> getSchemaJsonMap() {
    return ImmutableMap.of(
        namespace1 + "." + TABLE_1,
            ImmutableMap.<String, Object>builder()
                .put("transaction", true)
                .put("partition-key", Collections.singletonList("pk1"))
                .put("clustering-key", Arrays.asList("ck1 DESC", "ck2 ASC"))
                .put(
                    "columns",
                    ImmutableMap.<String, Object>builder()
                        .put("pk1", "INT")
                        .put("ck1", "INT")
                        .put("ck2", "TEXT")
                        .put("col1", "INT")
                        .put("col2", "BIGINT")
                        .put("col3", "FLOAT")
                        .put("col4", "DOUBLE")
                        .put("col5", "TEXT")
                        .put("col6", "BLOB")
                        .put("col7", "BOOLEAN")
                        .build())
                .put("secondary-index", Arrays.asList("col1", "col5"))
                .build(),
        namespace2 + "." + TABLE_2,
            ImmutableMap.<String, Object>builder()
                .put("transaction", false)
                .put("partition-key", Collections.singletonList("pk1"))
                .put("clustering-key", Collections.singletonList("ck1"))
                .put(
                    "columns",
                    ImmutableMap.of(
                        "pk1", "INT", "ck1", "INT", "col1", "INT", "col2", "BIGINT", "col3",
                        "FLOAT"))
                .build());
  }

  protected Map<String, Object> getAlteredSchemaJsonMap() {
    return ImmutableMap.of(
        namespace1 + "." + TABLE_1,
        ImmutableMap.<String, Object>builder()
            .put("transaction", true)
            .put("partition-key", Collections.singletonList("pk1"))
            .put("clustering-key", Arrays.asList("ck1 DESC", "ck2 ASC"))
            .put(
                "columns",
                ImmutableMap.<String, Object>builder()
                    .put("pk1", "INT")
                    .put("ck1", "INT")
                    .put("ck2", "TEXT")
                    .put("col1", "INT")
                    .put("col2", "BIGINT")
                    .put("col3", "FLOAT")
                    .put("col4", "DOUBLE")
                    .put("col5", "TEXT")
                    .put("col6", "BLOB")
                    .put("col7", "BOOLEAN")
                    .put("col8", "TEXT")
                    .put("col9", "BLOB")
                    .build())
            .put("secondary-index", Arrays.asList("col3", "col8"))
            .put("compaction-strategy", "LCS")
            .put("network-strategy", "SimpleStrategy")
            .put("replication-factor", "1")
            .build(),
        namespace2 + "." + TABLE_2,
        ImmutableMap.<String, Object>builder()
            .put("transaction", false)
            .put("partition-key", Collections.singletonList("pk1"))
            .put("clustering-key", Collections.singletonList("ck1"))
            .put(
                "columns",
                ImmutableMap.of(
                    "pk1", "INT", "ck1", "INT", "col1", "INT", "col2", "BIGINT", "col3", "FLOAT",
                    "col4", "TEXT"))
            .put("network-strategy", "SimpleStrategy")
            .put("replication-factor", "1")
            .build());
  }

  protected void writeSchemaFile(Path schemaFilePath, Map<String, Object> schemaJsonMap)
      throws IOException {
    Gson gson = new Gson();
    try (Writer writer = Files.newBufferedWriter(schemaFilePath)) {
      gson.toJson(schemaJsonMap, writer);
    }
  }

  protected List<String> getCommandArgsForCreation(Path configFilePath, Path schemaFilePath)
      throws Exception {
    return ImmutableList.of(
        "--config", configFilePath.toString(), "--schema-file", schemaFilePath.toString());
  }

  protected List<String> getCommandArgsForCreationWithCoordinator(
      Path configFilePath, Path schemaFilePath) throws Exception {
    return ImmutableList.<String>builder()
        .addAll(getCommandArgsForCreation(configFilePath, schemaFilePath))
        .add("--coordinator")
        .build();
  }

  protected List<String> getCommandArgsForReparation(Path configFilePath, Path schemaFilePath) {
    return ImmutableList.of(
        "--config",
        configFilePath.toString(),
        "--schema-file",
        schemaFilePath.toString(),
        "--repair-all");
  }

  protected List<String> getCommandArgsForReparationWithCoordinator(
      Path configFilePath, Path schemaFilePath) {
    return ImmutableList.<String>builder()
        .addAll(getCommandArgsForReparation(configFilePath, schemaFilePath))
        .add("--coordinator")
        .build();
  }

  protected List<String> getCommandArgsForDeletion(Path configFilePath, Path schemaFilePath)
      throws Exception {
    return ImmutableList.<String>builder()
        .addAll(getCommandArgsForCreation(configFilePath, schemaFilePath))
        .add("-D")
        .build();
  }

  protected List<String> getCommandArgsForDeletionWithCoordinator(
      Path configFilePath, Path schemaFilePath) throws Exception {
    return ImmutableList.<String>builder()
        .addAll(getCommandArgsForCreationWithCoordinator(configFilePath, schemaFilePath))
        .add("-D")
        .build();
  }

  protected List<String> getCommandArgsForAlteration(Path configFilePath, Path schemaFilePath)
      throws Exception {
    return ImmutableList.<String>builder()
        .addAll(getCommandArgsForCreation(configFilePath, schemaFilePath))
        .add("--alter")
        .build();
  }

  protected List<String> getCommandArgsForUpgrade(Path configFilePath) {
    return ImmutableList.of("--config", configFilePath.toString(), "--upgrade");
  }

  @AfterAll
  public void afterAll() throws Exception {
    try {
      dropTablesIfExist();
    } catch (Exception e) {
      logger.warn("Failed to drop tables", e);
    }

    try {
      if (storageAdmin != null) {
        storageAdmin.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close storage admin", e);
    }

    try {
      if (transactionAdmin != null) {
        transactionAdmin.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close transaction admin", e);
    }

    try {
      if (adminTestUtils != null) {
        adminTestUtils.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close admin test utils", e);
    }

    // Delete the files
    Files.delete(CONFIG_FILE_PATH);
    Files.delete(SCHEMA_FILE_PATH);
    Files.delete(ALTERED_SCHEMA_FILE_PATH);
  }

  private void dropTablesIfExist() throws ExecutionException {
    transactionAdmin.dropTable(namespace1, TABLE_1, true);
    transactionAdmin.dropNamespace(namespace1, true);
    transactionAdmin.dropCoordinatorTables(true);
    storageAdmin.dropTable(namespace2, TABLE_2, true);
    storageAdmin.dropNamespace(namespace2, true);
  }

  @Test
  public void createTablesThenDeleteTables_ShouldExecuteProperly() throws Exception {
    createTables_ShouldCreateTables();
    waitForCreationIfNecessary();
    deleteTables_ShouldDeleteTables();
  }

  private void createTables_ShouldCreateTables() throws Exception {
    // Act
    int exitCode = executeWithArgs(getCommandArgsForCreation(CONFIG_FILE_PATH, SCHEMA_FILE_PATH));

    // Assert
    assertThat(exitCode).isEqualTo(0);
    assertThat(transactionAdmin.tableExists(namespace1, TABLE_1)).isTrue();
    assertThat(storageAdmin.tableExists(namespace2, TABLE_2)).isTrue();
    assertThat(transactionAdmin.coordinatorTablesExist()).isFalse();
  }

  private void deleteTables_ShouldDeleteTables() throws Exception {
    // Act
    int exitCode = executeWithArgs(getCommandArgsForDeletion(CONFIG_FILE_PATH, SCHEMA_FILE_PATH));

    // Assert
    assertThat(exitCode).isEqualTo(0);
    assertThat(transactionAdmin.tableExists(namespace1, TABLE_1)).isFalse();
    assertThat(storageAdmin.tableExists(namespace2, TABLE_2)).isFalse();
    assertThat(transactionAdmin.coordinatorTablesExist()).isFalse();
  }

  @Test
  public void createTablesThenDeleteTablesWithCoordinator_ShouldExecuteProperly() throws Exception {
    createTables_ShouldCreateTablesWithCoordinator();
    waitForCreationIfNecessary();
    deleteTables_ShouldDeleteTablesWithCoordinator();
  }

  @Test
  public void createTablesThenDropTablesThenRepairAllWithoutCoordinator_ShouldExecuteProperly()
      throws Exception {
    // Arrange
    int exitCodeCreation =
        executeWithArgs(getCommandArgsForCreation(CONFIG_FILE_PATH, SCHEMA_FILE_PATH));
    assertThat(exitCodeCreation).isZero();
    adminTestUtils.dropNamespacesTable();
    adminTestUtils.dropTable(namespace1, TABLE_1);
    adminTestUtils.dropNamespace(namespace1);

    // Act
    int exitCodeReparation =
        executeWithArgs(getCommandArgsForReparation(CONFIG_FILE_PATH, SCHEMA_FILE_PATH));

    // Assert
    assertThat(exitCodeReparation).isZero();
    waitForCreationIfNecessary();
    assertThat(adminTestUtils.namespaceExists(namespace1)).isTrue();
    assertThat(adminTestUtils.namespaceExists(namespace2)).isTrue();
    assertThat(transactionAdmin.namespaceExists(namespace1)).isTrue();
    assertThat(storageAdmin.namespaceExists(namespace2)).isTrue();
    assertThat(adminTestUtils.tableExists(namespace1, TABLE_1)).isTrue();
    assertThat(adminTestUtils.tableExists(namespace2, TABLE_2)).isTrue();
    assertThat(transactionAdmin.getTableMetadata(namespace1, TABLE_1)).isEqualTo(TABLE_1_METADATA);
    assertThat(storageAdmin.getTableMetadata(namespace2, TABLE_2)).isEqualTo(TABLE_2_METADATA);
    assertThat(adminTestUtils.areTableAndMetadataForCoordinatorTablesPresent()).isFalse();
  }

  @Test
  public void createTablesThenDropTablesThenRepairAllWithCoordinator_ShouldExecuteProperly()
      throws Exception {
    // Arrange
    int exitCodeCreation =
        executeWithArgs(
            getCommandArgsForCreationWithCoordinator(CONFIG_FILE_PATH, SCHEMA_FILE_PATH));
    assertThat(exitCodeCreation).isZero();
    adminTestUtils.dropMetadataTable();
    adminTestUtils.dropTable(namespace1, TABLE_1);
    adminTestUtils.dropNamespace(namespace1);

    // Act
    int exitCodeReparation =
        executeWithArgs(
            getCommandArgsForReparationWithCoordinator(CONFIG_FILE_PATH, SCHEMA_FILE_PATH));

    // Assert
    assertThat(exitCodeReparation).isZero();
    waitForCreationIfNecessary();
    assertThat(adminTestUtils.namespaceExists(namespace1)).isTrue();
    assertThat(adminTestUtils.namespaceExists(namespace2)).isTrue();
    assertThat(transactionAdmin.namespaceExists(namespace1)).isTrue();
    assertThat(storageAdmin.namespaceExists(namespace2)).isTrue();
    assertThat(adminTestUtils.tableExists(namespace1, TABLE_1)).isTrue();
    assertThat(adminTestUtils.tableExists(namespace2, TABLE_2)).isTrue();
    assertThat(transactionAdmin.getTableMetadata(namespace1, TABLE_1)).isEqualTo(TABLE_1_METADATA);
    assertThat(storageAdmin.getTableMetadata(namespace2, TABLE_2)).isEqualTo(TABLE_2_METADATA);
    assertThat(adminTestUtils.areTableAndMetadataForCoordinatorTablesPresent()).isTrue();
  }

  @Test
  public void createTableThenAlterTables_ShouldExecuteProperly() throws Exception {
    // Arrange
    int exitCodeCreation =
        executeWithArgs(
            getCommandArgsForCreationWithCoordinator(CONFIG_FILE_PATH, SCHEMA_FILE_PATH));
    assertThat(exitCodeCreation).isZero();

    TableMetadata expectedTable1Metadata =
        TableMetadata.newBuilder(TABLE_1_METADATA)
            .addColumn("col8", DataType.TEXT)
            .addColumn("col9", DataType.BLOB)
            .removeSecondaryIndex("col1")
            .removeSecondaryIndex("col5")
            .addSecondaryIndex("col3")
            .addSecondaryIndex("col8")
            .build();
    TableMetadata expectedTable2Metadata =
        TableMetadata.newBuilder(TABLE_2_METADATA).addColumn("col4", DataType.TEXT).build();

    // Act
    int exitCodeAlteration =
        executeWithArgs(getCommandArgsForAlteration(CONFIG_FILE_PATH, ALTERED_SCHEMA_FILE_PATH));

    // Assert
    assertThat(exitCodeAlteration).isZero();
    assertThat(transactionAdmin.getTableMetadata(namespace1, TABLE_1))
        .isEqualTo(expectedTable1Metadata);
    assertThat(storageAdmin.getTableMetadata(namespace2, TABLE_2))
        .isEqualTo(expectedTable2Metadata);
  }

  @Test
  public void createThenDropNamespacesTableThenUpgrade_ShouldCreateNamespacesTableCorrectly()
      throws Exception {
    // Arrange
    int exitCodeCreation =
        executeWithArgs(
            getCommandArgsForCreationWithCoordinator(CONFIG_FILE_PATH, SCHEMA_FILE_PATH));
    assertThat(exitCodeCreation).isZero();
    adminTestUtils.dropNamespacesTable();

    // Act
    int exitCodeUpgrade = executeWithArgs(getCommandArgsForUpgrade(CONFIG_FILE_PATH));

    // Assert
    assertThat(exitCodeUpgrade).isZero();
    waitForCreationIfNecessary();
    assertThat(transactionAdmin.getNamespaceNames())
        .containsOnly(namespace1, namespace2, systemNamespaceName);
  }

  private void createTables_ShouldCreateTablesWithCoordinator() throws Exception {
    // Act
    int exitCode =
        executeWithArgs(
            getCommandArgsForCreationWithCoordinator(CONFIG_FILE_PATH, SCHEMA_FILE_PATH));

    // Assert
    assertThat(exitCode).isEqualTo(0);
    assertThat(transactionAdmin.tableExists(namespace1, TABLE_1)).isTrue();
    assertThat(storageAdmin.tableExists(namespace2, TABLE_2)).isTrue();
    assertThat(transactionAdmin.coordinatorTablesExist()).isTrue();
  }

  private void deleteTables_ShouldDeleteTablesWithCoordinator() throws Exception {
    // Act
    int exitCode =
        executeWithArgs(
            getCommandArgsForDeletionWithCoordinator(CONFIG_FILE_PATH, SCHEMA_FILE_PATH));

    // Assert
    assertThat(exitCode).isEqualTo(0);
    assertThat(transactionAdmin.tableExists(namespace1, TABLE_1)).isFalse();
    assertThat(storageAdmin.tableExists(namespace2, TABLE_2)).isFalse();
    assertThat(transactionAdmin.coordinatorTablesExist()).isFalse();
  }

  private int executeWithArgs(List<String> args) {
    return SchemaLoader.mainInternal(args.toArray(new String[0]));
  }

  protected void waitForCreationIfNecessary() {
    // Do nothing
  }
}
