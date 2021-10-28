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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public abstract class SchemaLoaderIntegrationTestBase {

  protected static final String SCHEMA_FILE =
      System.getProperty("user.dir") + "/schema-loader/sample_data/schema_sample.json";
  protected static final String CONFIG_FILE = "config.properties";
  static DistributedStorageAdmin admin;
  static ConsensusCommitAdmin consensusCommitAdmin;
  static List<Table> tables;
  private static boolean initialized;

  @Before
  public void setUp() throws Exception {
    if (!initialized) {
      initialize();
      DatabaseConfig databaseConfig = getDatabaseConfig();
      StorageFactory factory = new StorageFactory(databaseConfig);
      admin = factory.getAdmin();
      consensusCommitAdmin =
          new ConsensusCommitAdmin(
              admin, new ConsensusCommitConfig(databaseConfig.getProperties()));
      tables = SchemaParser.parse(SCHEMA_FILE, Collections.emptyMap());
      initialized = true;
    }
  }

  void initialize() throws Exception {}

  protected abstract DatabaseConfig getDatabaseConfig();

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
    // Arrange
    ProcessBuilder processBuilder = new ProcessBuilder(args);

    // Act
    Process process = processBuilder.start();
    int exitCode = process.waitFor();

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
    // Arrange
    ProcessBuilder processBuilder = new ProcessBuilder(args);

    // Act
    Process process = processBuilder.start();
    int exitCode = process.waitFor();

    // Assert
    assertTableDeletion(exitCode);
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
