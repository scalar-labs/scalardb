package com.scalar.db.schemaloader;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdmin;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import com.scalar.db.util.AdminTestUtils;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class SchemaLoaderIntegrationTestBase {
  private static final String CONFIG_FILE = "config.properties";
  private static final String SCHEMA_FILE = "schema.json";

  private static final String NAMESPACE_1 = "integration_testing_schema_loader1";
  private static final String TABLE_1 = "test_table2";
  private static final String NAMESPACE_2 = "integration_testing_schema_loader2";
  private static final String TABLE_2 = "test_table3";
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
  private static final String schemaLoaderJarPath =
      System.getProperty("scalardb.schemaloader.jar_path");

  private DistributedStorageAdmin admin;
  private ConsensusCommitAdmin consensusCommitAdmin;
  private String namespace1;
  private String namespace2;

  @BeforeAll
  public void beforeAll() throws Exception {
    initialize();
    Properties properties = getProperties();
    namespace1 = getNamespace1();
    namespace2 = getNamespace2();
    writeConfigFile(properties);
    Map<String, Object> schemaJsonMap = getSchemaJsonMap();
    writeSchemaFile(schemaJsonMap);
    StorageFactory factory = StorageFactory.create(properties);
    admin = factory.getAdmin();
    consensusCommitAdmin = new ConsensusCommitAdmin(admin, new DatabaseConfig(properties));
  }

  @BeforeEach
  public void setUp() throws ExecutionException {
    dropTablesIfExist();
  }

  protected void initialize() throws Exception {}

  protected abstract Properties getProperties();

  protected void writeConfigFile(Properties properties) throws IOException {
    try (FileOutputStream fileOutputStream = new FileOutputStream(CONFIG_FILE)) {
      properties.store(fileOutputStream, null);
    }
  }

  protected String getNamespace1() {
    return NAMESPACE_1;
  }

  protected String getNamespace2() {
    return NAMESPACE_2;
  }

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
                .put("compaction-strategy", "LCS")
                .put("network-strategy", "NetworkTopologyStrategy")
                .put("replication-factor", "1")
                .put("ru", 5000)
                .build(),
        namespace2 + "." + TABLE_2,
            ImmutableMap.<String, Object>builder()
                .put("partition-key", Collections.singletonList("pk1"))
                .put("clustering-key", Collections.singletonList("ck1"))
                .put(
                    "columns",
                    ImmutableMap.of(
                        "pk1", "INT", "ck1", "INT", "col1", "INT", "col2", "BIGINT", "col3",
                        "FLOAT"))
                .put("network-strategy", "NetworkTopologyStrategy")
                .put("replication-factor", "1")
                .build());
  }

  protected void writeSchemaFile(Map<String, Object> schemaJsonMap) throws IOException {
    Gson gson = new Gson();
    try (Writer writer =
        new OutputStreamWriter(new FileOutputStream(SCHEMA_FILE), StandardCharsets.UTF_8)) {
      gson.toJson(schemaJsonMap, writer);
    }
  }

  protected List<String> getCommandArgsForCreation(String configFile, String schemaFile)
      throws Exception {
    return ImmutableList.of("--config", configFile, "--schema-file", schemaFile);
  }

  protected List<String> getCommandArgsForCreationWithCoordinator(
      String configFile, String schemaFile) throws Exception {
    return ImmutableList.<String>builder()
        .addAll(getCommandArgsForCreation(configFile, schemaFile))
        .add("--coordinator")
        .build();
  }

  protected List<String> getCommandArgsForTableReparation(String configFile, String schemaFile) {
    return ImmutableList.of("--config", configFile, "--schema-file", schemaFile, "--repair-all");
  }

  protected List<String> getCommandArgsForTableReparationWithCoordinator(
      String configFile, String schemaFile) throws Exception {
    return ImmutableList.<String>builder()
        .addAll(getCommandArgsForTableReparation(configFile, schemaFile))
        .add("--coordinator")
        .build();
  }

  protected List<String> getCommandArgsForDeletion(String configFile, String schemaFile)
      throws Exception {
    return ImmutableList.<String>builder()
        .addAll(getCommandArgsForCreation(configFile, schemaFile))
        .add("-D")
        .build();
  }

  protected List<String> getCommandArgsForDeletionWithCoordinator(
      String configFile, String schemaFile) throws Exception {
    return ImmutableList.<String>builder()
        .addAll(getCommandArgsForCreationWithCoordinator(configFile, schemaFile))
        .add("-D")
        .build();
  }

  @AfterAll
  public void afterAll() throws ExecutionException {
    dropTablesIfExist();
    admin.close();

    // Delete the files
    if (!new File(CONFIG_FILE).delete()) {
      System.err.println("failed to delete " + CONFIG_FILE);
    }
    if (!new File(SCHEMA_FILE).delete()) {
      System.err.println("failed to delete " + SCHEMA_FILE);
    }
  }

  private void dropTablesIfExist() throws ExecutionException {
    consensusCommitAdmin.dropTable(namespace1, TABLE_1, true);
    consensusCommitAdmin.dropNamespace(namespace1, true);
    if (consensusCommitAdmin.coordinatorTablesExist()) {
      consensusCommitAdmin.dropCoordinatorTables();
    }
    admin.dropTable(namespace2, TABLE_2, true);
    admin.dropNamespace(namespace2, true);
  }

  @Test
  public void createTablesThenDeleteTables_ShouldExecuteProperly() throws Exception {
    createTables_ShouldCreateTables();
    deleteTables_ShouldDeleteTables();
  }

  private void createTables_ShouldCreateTables() throws Exception {
    // Act
    int exitCode = executeCommandWithArgs(getCommandArgsForCreation(CONFIG_FILE, SCHEMA_FILE));

    // Assert
    assertThat(exitCode).isEqualTo(0);
    assertThat(consensusCommitAdmin.tableExists(namespace1, TABLE_1)).isTrue();
    assertThat(admin.tableExists(namespace2, TABLE_2)).isTrue();
    assertThat(consensusCommitAdmin.coordinatorTablesExist()).isFalse();
  }

  private void deleteTables_ShouldDeleteTables() throws Exception {
    // Act
    int exitCode = executeCommandWithArgs(getCommandArgsForDeletion(CONFIG_FILE, SCHEMA_FILE));

    // Assert
    assertThat(exitCode).isEqualTo(0);
    assertThat(consensusCommitAdmin.tableExists(namespace1, TABLE_1)).isFalse();
    assertThat(admin.tableExists(namespace2, TABLE_2)).isFalse();
    assertThat(consensusCommitAdmin.coordinatorTablesExist()).isFalse();
  }

  @Test
  public void createTablesThenDeleteTablesWithCoordinator_ShouldExecuteProperly() throws Exception {
    createTables_ShouldCreateTablesWithCoordinator();
    deleteTables_ShouldDeleteTablesWithCoordinator();
  }

  @Test
  public void createTableThenDropMetadataTableThenRepairTables_ShouldExecuteProperly()
      throws Exception {
    // Arrange
    int exitCodeCreation =
        executeCommandWithArgs(getCommandArgsForCreation(CONFIG_FILE, SCHEMA_FILE));
    assertThat(exitCodeCreation).isZero();
    dropMetadataTable();

    // Act
    int exitCodeReparation =
        executeCommandWithArgs(getCommandArgsForTableReparation(CONFIG_FILE, SCHEMA_FILE));

    // Assert
    assertThat(exitCodeReparation).isZero();
    assertThat(consensusCommitAdmin.getTableMetadata(namespace1, TABLE_1))
        .isEqualTo(TABLE_1_METADATA);
    assertThat(admin.getTableMetadata(namespace2, TABLE_2)).isEqualTo(TABLE_2_METADATA);
  }

  @Test
  public void
      createTableThenDropMetadataTableThenRepairTablesWithCoordinator_ShouldExecuteProperly()
          throws Exception {
    // Arrange
    int exitCodeCreation =
        executeCommandWithArgs(getCommandArgsForCreationWithCoordinator(CONFIG_FILE, SCHEMA_FILE));
    assertThat(exitCodeCreation).isZero();
    dropMetadataTable();

    // Act
    int exitCodeReparation =
        executeCommandWithArgs(
            getCommandArgsForTableReparationWithCoordinator(CONFIG_FILE, SCHEMA_FILE));

    // Assert
    assertThat(exitCodeReparation).isZero();
    assertThat(consensusCommitAdmin.getTableMetadata(namespace1, TABLE_1))
        .isEqualTo(TABLE_1_METADATA);
    assertThat(admin.getTableMetadata(namespace2, TABLE_2)).isEqualTo(TABLE_2_METADATA);
    assertTableMetadataForCoordinatorTableArePresent();
  }

  private void createTables_ShouldCreateTablesWithCoordinator() throws Exception {
    // Act
    int exitCode =
        executeCommandWithArgs(getCommandArgsForCreationWithCoordinator(CONFIG_FILE, SCHEMA_FILE));

    // Assert
    assertThat(exitCode).isEqualTo(0);
    assertThat(consensusCommitAdmin.tableExists(namespace1, TABLE_1)).isTrue();
    assertThat(admin.tableExists(namespace2, TABLE_2)).isTrue();
    assertThat(consensusCommitAdmin.coordinatorTablesExist()).isTrue();
  }

  private void deleteTables_ShouldDeleteTablesWithCoordinator() throws Exception {
    // Act
    int exitCode =
        executeCommandWithArgs(getCommandArgsForDeletionWithCoordinator(CONFIG_FILE, SCHEMA_FILE));

    // Assert
    assertThat(exitCode).isEqualTo(0);
    assertThat(consensusCommitAdmin.tableExists(namespace1, TABLE_1)).isFalse();
    assertThat(admin.tableExists(namespace2, TABLE_2)).isFalse();
    assertThat(consensusCommitAdmin.coordinatorTablesExist()).isFalse();
  }

  private int executeCommandWithArgs(List<String> args) throws Exception {
    List<String> command =
        ImmutableList.<String>builder()
            .add("java")
            .add("-jar")
            .add(schemaLoaderJarPath)
            .addAll(args)
            .build();
    ProcessBuilder processBuilder = new ProcessBuilder(command);

    Process process = processBuilder.start();
    try (final BufferedReader input =
        new BufferedReader(
            new InputStreamReader(process.getErrorStream(), Charset.defaultCharset()))) {
      String line;
      while ((line = input.readLine()) != null) {
        System.out.println(line);
      }
    }

    return process.waitFor();
  }

  protected void assertTableMetadataForCoordinatorTableArePresent() throws Exception {
    assertTableMetadataForCoordinatorTableArePresentForStorage(getProperties());
  }

  protected void assertTableMetadataForCoordinatorTableArePresentForStorage(
      Properties coordinatorStorageProperties) throws Exception {
    String coordinatorNamespace =
        new ConsensusCommitConfig(new DatabaseConfig(coordinatorStorageProperties))
            .getCoordinatorNamespace()
            .orElse(Coordinator.NAMESPACE);
    String coordinatorTable = Coordinator.TABLE;
    // Use the DistributedStorageAdmin instead of the DistributedTransactionAdmin because the latter
    // expects the table to hold transaction table metadata columns which is not the case for the
    // coordinator table
    DistributedStorageAdmin storageAdmin =
        StorageFactory.create(coordinatorStorageProperties).getStorageAdmin();

    assertThat(storageAdmin.getTableMetadata(coordinatorNamespace, coordinatorTable))
        .isEqualTo(Coordinator.TABLE_METADATA);

    storageAdmin.close();
  }

  protected void dropMetadataTable() throws Exception {
    AdminTestUtils adminTestUtils = AdminTestUtils.create(getProperties());
    adminTestUtils.dropMetadataTable();
  }
}
