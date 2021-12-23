package com.scalar.db.schemaloader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.schemaloader.schema.SchemaParser;
import com.scalar.db.schemaloader.schema.Table;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdmin;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.Before;
import org.junit.Test;

public abstract class SchemaLoaderIntegrationTestBase {
  protected static final String SCHEMA_FILE =
      System.getProperty("user.dir") + "/schema-loader/src/integration-test/resources/schema.json";
  protected static final String CONFIG_FILE = "config.properties";
  DistributedStorageAdmin admin;
  ConsensusCommitAdmin consensusCommitAdmin;
  protected List<Table> tables;
  protected DatabaseConfig config;
  private boolean initialized;

  @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
  @Before
  public void setUp() throws Exception {
    if (!initialized) {
      initialize();
      Properties properties = config.getProperties();
      try (final FileOutputStream fileOutputStream = new FileOutputStream(CONFIG_FILE)) {
        properties.store(fileOutputStream, null);
      }
      StorageFactory factory = new StorageFactory(config);
      admin = factory.getAdmin();
      consensusCommitAdmin =
          new ConsensusCommitAdmin(admin, new ConsensusCommitConfig(config.getProperties()));
      parsingTables();
      initialized = true;
    }
  }

  protected void initialize() throws Exception {}

  protected void parsingTables() throws Exception {
    tables = SchemaParser.parse(Paths.get(SCHEMA_FILE), Collections.emptyMap());
  }

  protected abstract List<String> getStorageSpecificCreationCommandArgs();

  protected List<String> getStorageSpecificDeletionCommandArgs() {
    List<String> args = new ArrayList<>(getStorageSpecificCreationCommandArgs());
    args.add("-D");
    return args;
  }

  protected List<String> getSchemaLoaderCreationCommandArgs() {
    return ImmutableList.of(
        "java",
        "-jar",
        "scalardb-schema-loader.jar",
        "--config",
        CONFIG_FILE,
        "--schema-file",
        SCHEMA_FILE,
        "--coordinator");
  }

  protected List<String> getSchemaLoaderDeletionCommandArgs() {
    List<String> args = new ArrayList<>(getSchemaLoaderCreationCommandArgs());
    args.add("-D");
    return args;
  }

  @Test
  public void storageSpecificCommand_GivenProperArgs_ShouldCreateTablesThenDeleteTables()
      throws Exception {
    createTables_GivenProperSchemaFileAndStorageSpecificCommandArgs_ShouldCreateTables();
    deleteTables_GivenProperSchemaFileAndStorageSpecificCommandArgs_ShouldDeleteTables();
  }

  @Test
  public void schemaLoaderCommand_GivenProperArgs_ShouldCreateTablesThenDeleteTables()
      throws Exception {
    createTables_GivenProperSchemaFileAndSchemaLoaderCommandArgs_ShouldCreateTables();
    deleteTables_GivenProperSchemaFileAndSchemaLoaderCommandArgs_ShouldDeleteTables();
  }

  public void createTables_GivenProperSchemaFileAndStorageSpecificCommandArgs_ShouldCreateTables()
      throws Exception {
    createTables_GivenProperSchemaFileAndCommandArgs_ShouldCreateTables(
        getStorageSpecificCreationCommandArgs());
  }

  public void createTables_GivenProperSchemaFileAndSchemaLoaderCommandArgs_ShouldCreateTables()
      throws Exception {
    createTables_GivenProperSchemaFileAndCommandArgs_ShouldCreateTables(
        getSchemaLoaderCreationCommandArgs());
  }

  private void createTables_GivenProperSchemaFileAndCommandArgs_ShouldCreateTables(
      List<String> args) throws Exception {
    // Act
    int exitCode = executeCommandWithArgs(args);

    // Assert
    assertTableCreation(exitCode);
  }

  public void deleteTables_GivenProperSchemaFileAndStorageSpecificCommandArgs_ShouldDeleteTables()
      throws Exception {
    deleteTables_GivenProperSchemaFileAndCommandArgs_ShouldDeleteTables(
        getStorageSpecificDeletionCommandArgs());
  }

  public void deleteTables_GivenProperSchemaFileAndSchemaLoaderCommandArgs_ShouldDeleteTables()
      throws Exception {
    deleteTables_GivenProperSchemaFileAndCommandArgs_ShouldDeleteTables(
        getSchemaLoaderDeletionCommandArgs());
  }

  private void deleteTables_GivenProperSchemaFileAndCommandArgs_ShouldDeleteTables(
      List<String> args) throws Exception {
    // Act
    int exitCode = executeCommandWithArgs(args);

    // Assert
    assertTableDeletion(exitCode);
  }

  private int executeCommandWithArgs(List<String> args) throws Exception {
    // Arrange
    ProcessBuilder processBuilder = new ProcessBuilder(args);

    // Act
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

  private void assertTableCreation(int exitCode) throws ExecutionException {
    assertEquals("Table creation process exited with wrong exit code", 0, exitCode);
    for (Table table : tables) {
      assertTrue(admin.namespaceExists(table.getNamespace()));
      assertTrue(admin.tableExists(table.getNamespace(), table.getTable()));
    }
    assertTrue(consensusCommitAdmin.coordinatorTableExists());
  }

  private void assertTableDeletion(int exitCode) throws ExecutionException {
    assertEquals("Table deletion process exited with wrong exit code", 0, exitCode);
    for (Table table : tables) {
      assertFalse(admin.namespaceExists(table.getNamespace()));
      assertFalse(admin.tableExists(table.getNamespace(), table.getTable()));
    }
    assertFalse(consensusCommitAdmin.coordinatorTableExists());
  }
}
