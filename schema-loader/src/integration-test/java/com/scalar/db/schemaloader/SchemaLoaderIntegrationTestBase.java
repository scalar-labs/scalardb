package com.scalar.db.schemaloader;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdmin;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

@SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
public abstract class SchemaLoaderIntegrationTestBase {
  private static final String CONFIG_FILE = "config.properties";
  private static final String SCHEMA_FILE = "schema.json";

  private static final String NAMESPACE_1 = "integration_testing_schema_loader1";
  private static final String TABLE_1 = "test_table2";
  private static final String NAMESPACE_2 = "integration_testing_schema_loader2";
  private static final String TABLE_2 = "test_table3";

  private static final String schemaLoaderJarPath =
      System.getProperty("scalardb.schemaloader.jar_path");

  private static boolean initialized;
  private static DistributedStorageAdmin admin;
  private static ConsensusCommitAdmin consensusCommitAdmin;
  private static String namespace1;
  private static String namespace2;

  @Before
  public void setUp() throws Exception {
    if (!initialized) {
      initialize();
      DatabaseConfig config = getDatabaseConfig();
      namespace1 = getNamespace1();
      namespace2 = getNamespace2();
      writeConfigFile(config.getProperties());
      Map<String, Object> schemaJsonMap = getSchemaJsonMap();
      writeSchemaFile(schemaJsonMap);
      StorageFactory factory = new StorageFactory(config);
      admin = factory.getAdmin();
      consensusCommitAdmin =
          new ConsensusCommitAdmin(admin, new ConsensusCommitConfig(config.getProperties()));
      initialized = true;
    }
    dropTablesIfExist();
  }

  protected void initialize() throws Exception {}

  protected abstract DatabaseConfig getDatabaseConfig();

  @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
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

  protected List<String> getCommandArgsForCreationWithCoordinatorTable(
      String configFile, String schemaFile) throws Exception {
    return ImmutableList.<String>builder()
        .addAll(getCommandArgsForCreation(configFile, schemaFile))
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

  protected List<String> getCommandArgsForDeletionWithCoordinatorTable(
      String configFile, String schemaFile) throws Exception {
    return ImmutableList.<String>builder()
        .addAll(getCommandArgsForCreationWithCoordinatorTable(configFile, schemaFile))
        .add("-D")
        .build();
  }

  @AfterClass
  public static void tearDownAfterClass() throws ExecutionException {
    dropTablesIfExist();
    admin.close();

    // Delete the files
    if (!new File(CONFIG_FILE).delete()) {
      System.err.println("failed to delete " + CONFIG_FILE);
    }
    if (!new File(SCHEMA_FILE).delete()) {
      System.err.println("failed to delete " + SCHEMA_FILE);
    }

    initialized = false;
  }

  private static void dropTablesIfExist() throws ExecutionException {
    admin.dropTable(namespace1, TABLE_1, true);
    admin.dropNamespace(namespace1, true);
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
    assertThat(admin.tableExists(namespace1, TABLE_1)).isTrue();
    assertThat(admin.tableExists(namespace2, TABLE_2)).isTrue();
    assertThat(consensusCommitAdmin.coordinatorTableExists()).isFalse();
  }

  private void deleteTables_ShouldDeleteTables() throws Exception {
    // Act
    int exitCode = executeCommandWithArgs(getCommandArgsForDeletion(CONFIG_FILE, SCHEMA_FILE));

    // Assert
    assertThat(exitCode).isEqualTo(0);
    assertThat(admin.tableExists(namespace1, TABLE_1)).isFalse();
    assertThat(admin.tableExists(namespace2, TABLE_2)).isFalse();
    assertThat(consensusCommitAdmin.coordinatorTableExists()).isFalse();
  }

  @Test
  public void createTablesThenDeleteTablesWithCoordinatorTable_ShouldExecuteProperly()
      throws Exception {
    createTables_ShouldCreateTablesWithCoordinatorTable();
    deleteTables_ShouldDeleteTablesWithCoordinatorTable();
  }

  private void createTables_ShouldCreateTablesWithCoordinatorTable() throws Exception {
    // Act
    int exitCode =
        executeCommandWithArgs(
            getCommandArgsForCreationWithCoordinatorTable(CONFIG_FILE, SCHEMA_FILE));

    // Assert
    assertThat(exitCode).isEqualTo(0);
    assertThat(admin.tableExists(namespace1, TABLE_1)).isTrue();
    assertThat(admin.tableExists(namespace2, TABLE_2)).isTrue();
    assertThat(consensusCommitAdmin.coordinatorTableExists()).isTrue();
  }

  private void deleteTables_ShouldDeleteTablesWithCoordinatorTable() throws Exception {
    // Act
    int exitCode =
        executeCommandWithArgs(
            getCommandArgsForDeletionWithCoordinatorTable(CONFIG_FILE, SCHEMA_FILE));

    // Assert
    assertThat(exitCode).isEqualTo(0);
    assertThat(admin.tableExists(namespace1, TABLE_1)).isFalse();
    assertThat(admin.tableExists(namespace2, TABLE_2)).isFalse();
    assertThat(consensusCommitAdmin.coordinatorTableExists()).isFalse();
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
}
