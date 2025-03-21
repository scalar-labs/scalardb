package com.scalar.db.schemaloader;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.service.TransactionFactory;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
public abstract class SchemaLoaderImportIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(SchemaLoaderImportIntegrationTestBase.class);
  private static final String TEST_NAME = "schema_loader_import";
  private static final Path CONFIG_FILE_PATH = Paths.get("config.properties").toAbsolutePath();
  private static final Path IMPORT_SCHEMA_FILE_PATH =
      Paths.get("import_schema.json").toAbsolutePath();

  private static final String NAMESPACE_1 = "int_test_" + TEST_NAME + "1";
  private static final String TABLE_1 = "test_table1";
  private static final String NAMESPACE_2 = "int_test_" + TEST_NAME + "2";
  private static final String TABLE_2 = "test_table2";

  private DistributedStorageAdmin storageAdmin;
  private DistributedTransactionAdmin transactionAdmin;
  private String namespace1;
  private String namespace2;

  @BeforeAll
  public void beforeAll() throws Exception {
    initialize(TEST_NAME);
    Properties properties = getProperties(TEST_NAME);
    namespace1 = getNamespace1();
    namespace2 = getNamespace2();
    writeConfigFile(properties);
    writeSchemaFile(IMPORT_SCHEMA_FILE_PATH, getImportSchemaJsonMap());
    StorageFactory factory = StorageFactory.create(properties);
    storageAdmin = factory.getStorageAdmin();
    TransactionFactory transactionFactory = TransactionFactory.create(properties);
    transactionAdmin = transactionFactory.getTransactionAdmin();
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

  protected Map<String, Object> getImportSchemaJsonMap() {
    return ImmutableMap.of(
        namespace1 + "." + TABLE_1,
        ImmutableMap.<String, Object>builder()
            .put("transaction", true)
            .put("override-columns-type", getImportableTableOverrideColumnsType())
            .build(),
        namespace2 + "." + TABLE_2,
        ImmutableMap.<String, Object>builder().put("transaction", false).build());
  }

  protected void writeSchemaFile(Path schemaFilePath, Map<String, Object> schemaJsonMap)
      throws IOException {
    Gson gson = new Gson();
    try (Writer writer = Files.newBufferedWriter(schemaFilePath)) {
      gson.toJson(schemaJsonMap, writer);
    }
  }

  protected List<String> getCommandArgsForImport(Path configFilePath, Path schemaFilePath) {
    return ImmutableList.of(
        "--config",
        configFilePath.toString(),
        "--schema-file",
        schemaFilePath.toString(),
        "--import");
  }

  @AfterAll
  public void afterAll() throws Exception {
    try {
      dropTablesIfExist();
    } catch (Exception e) {
      logger.warn("Failed to drop tables", e);
    }

    try {
      if (transactionAdmin != null) {
        transactionAdmin.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close transaction admin", e);
    }

    try {
      if (storageAdmin != null) {
        storageAdmin.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close storage admin", e);
    }

    // Delete the files
    Files.delete(CONFIG_FILE_PATH);
    Files.delete(IMPORT_SCHEMA_FILE_PATH);
  }

  private void dropTablesIfExist() throws Exception {
    transactionAdmin.dropTable(namespace1, TABLE_1, true);
    transactionAdmin.dropNamespace(namespace1, true);
    storageAdmin.dropTable(namespace2, TABLE_2, true);
    storageAdmin.dropNamespace(namespace2, true);
  }

  protected abstract void createImportableTable(String namespace, String table) throws Exception;

  protected abstract void createNonImportableTable(String namespace, String table) throws Exception;

  protected abstract void dropNonImportableTable(String namespace, String table) throws Exception;

  protected abstract Map<String, DataType> getImportableTableOverrideColumnsType();

  protected abstract TableMetadata getImportableTableMetadata(boolean hasTypeOverride);

  protected void waitForDifferentSessionDdl() {
    // No wait by default.
  }

  @Test
  public void importTables_ImportableTablesGiven_ShouldImportProperly() throws Exception {
    // Arrange
    waitForDifferentSessionDdl();
    transactionAdmin.createNamespace(namespace1);

    waitForDifferentSessionDdl();
    storageAdmin.createNamespace(namespace2);

    waitForDifferentSessionDdl();
    // TABLE_1 set options to override column types.
    createImportableTable(namespace1, TABLE_1);

    waitForDifferentSessionDdl();
    // TABLE_2 does not set options to override column types.
    createImportableTable(namespace2, TABLE_2);

    // Act
    waitForDifferentSessionDdl();
    int exitCode =
        executeWithArgs(getCommandArgsForImport(CONFIG_FILE_PATH, IMPORT_SCHEMA_FILE_PATH));

    // Assert
    assertThat(exitCode).isEqualTo(0);
    assertThat(transactionAdmin.tableExists(namespace1, TABLE_1)).isTrue();
    assertThat(storageAdmin.tableExists(namespace2, TABLE_2)).isTrue();
    assertThat(transactionAdmin.getTableMetadata(namespace1, TABLE_1))
        .isEqualTo(getImportableTableMetadata(true));
    assertThat(storageAdmin.getTableMetadata(namespace2, TABLE_2))
        .isEqualTo(getImportableTableMetadata(false));
    assertThat(transactionAdmin.coordinatorTablesExist()).isFalse();
  }

  @Test
  public void importTables_ImportableTablesAndNonRelatedSameNameTableGiven_ShouldImportProperly()
      throws Exception {
    // Arrange
    waitForDifferentSessionDdl();
    transactionAdmin.createNamespace(namespace1);

    waitForDifferentSessionDdl();
    storageAdmin.createNamespace(namespace2);

    waitForDifferentSessionDdl();
    createImportableTable(namespace1, TABLE_1);

    waitForDifferentSessionDdl();
    createImportableTable(namespace2, TABLE_2);

    // Create non-related tables.
    waitForDifferentSessionDdl();
    createNonImportableTable(namespace1, TABLE_2);
    waitForDifferentSessionDdl();
    createNonImportableTable(namespace2, TABLE_1);

    try {
      // Act
      waitForDifferentSessionDdl();
      int exitCode =
          executeWithArgs(getCommandArgsForImport(CONFIG_FILE_PATH, IMPORT_SCHEMA_FILE_PATH));

      // Assert
      assertThat(exitCode).isEqualTo(0);
      assertThat(transactionAdmin.tableExists(namespace1, TABLE_1)).isTrue();
      assertThat(storageAdmin.tableExists(namespace2, TABLE_2)).isTrue();
      assertThat(transactionAdmin.coordinatorTablesExist()).isFalse();
    } finally {
      try {
        dropNonImportableTable(namespace1, TABLE_2);
      } catch (Exception e) {
        logger.warn("Failed to drop non-importable table. {}.{}", namespace1, TABLE_2, e);
      }

      try {
        dropNonImportableTable(namespace2, TABLE_1);
      } catch (Exception e) {
        logger.warn("Failed to drop non-importable table. {}.{}", namespace2, TABLE_1, e);
      }
    }
  }

  @Test
  public void importTables_NonImportableTablesGiven_ShouldThrowIllegalArgumentException()
      throws Exception {
    // Arrange
    waitForDifferentSessionDdl();
    transactionAdmin.createNamespace(namespace1);

    waitForDifferentSessionDdl();
    storageAdmin.createNamespace(namespace2);

    waitForDifferentSessionDdl();
    createNonImportableTable(namespace1, TABLE_1);

    waitForDifferentSessionDdl();
    createNonImportableTable(namespace2, TABLE_2);

    // Act
    waitForDifferentSessionDdl();
    int exitCode =
        executeWithArgs(getCommandArgsForImport(CONFIG_FILE_PATH, IMPORT_SCHEMA_FILE_PATH));

    // Assert
    assertThat(exitCode).isEqualTo(1);
    dropNonImportableTable(namespace1, TABLE_1);
    dropNonImportableTable(namespace2, TABLE_2);
  }

  protected int executeWithArgs(List<String> args) {
    return SchemaLoader.mainInternal(args.toArray(new String[0]));
  }
}
