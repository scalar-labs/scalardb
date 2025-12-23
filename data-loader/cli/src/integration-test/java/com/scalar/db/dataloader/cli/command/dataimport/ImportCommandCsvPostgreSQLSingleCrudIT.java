package com.scalar.db.dataloader.cli.command.dataimport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.scalar.db.dataloader.cli.BasePostgreSQLIntegrationTest;
import com.scalar.db.dataloader.cli.TestDataValidationHelper;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.MountableFile;
import picocli.CommandLine;

/**
 * Integration tests for CSV import functionality using PostgreSQL with single-crud-operation
 * transaction manager.
 *
 * <p>These tests verify that CSV import operations work correctly when using the
 * single-crud-operation transaction manager. This transaction manager provides storage-only mode
 * without distributed transactions, suitable for simple CRUD operations.
 *
 * <p>Tests various CSV import scenarios including:
 *
 * <ul>
 *   <li>Different import modes (INSERT, UPDATE, UPSERT)
 *   <li>Storage mode operations
 *   <li>Control file validation (FULL, MAPPED)
 *   <li>Custom delimiters and headers
 *   <li>Data validation after import
 * </ul>
 *
 * <p>These tests use a shared PostgreSQL container initialized with test data. Each test starts
 * with a clean state (tables are truncated before each test) to ensure test isolation.
 *
 * @see BasePostgreSQLIntegrationTest for shared test infrastructure
 */
public class ImportCommandCsvPostgreSQLSingleCrudIT extends BasePostgreSQLIntegrationTest {

  @Override
  protected String getTransactionManagerType() {
    return TRANSACTION_MANAGER_SINGLE_CRUD;
  }

  @BeforeAll
  static void setupContainer() {
    // Copy initialization SQL file to container
    postgres.withCopyFileToContainer(
        MountableFile.forClasspathResource("init_postgres_import.sql"),
        "/docker-entrypoint-initdb.d/init_postgres.sql");
  }

  @Test
  void testImportFromFileWithSingleCrudTransactionManager_ShouldSucceed() throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("import_data/import_single.csv"))
                .toURI());
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      NAMESPACE,
      "--table",
      TABLE_EMPLOYEE,
      "--log-dir",
      tempDir.toString(),
      "--format",
      "CSV",
      "--import-mode",
      "UPSERT",
      "--file",
      filePath.toString()
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertThat(exitCode).isEqualTo(0);
    // Verify data was imported - should have 11 records (0-10)
    int recordCount =
        TestDataValidationHelper.countRecords(configFilePath, NAMESPACE, TABLE_EMPLOYEE);
    assertTrue(recordCount >= 11, "Expected at least 11 records, but found " + recordCount);
    // Verify a specific record exists
    assertTrue(
        TestDataValidationHelper.verifyRecordExists(configFilePath, NAMESPACE, TABLE_EMPLOYEE, 0),
        "Record with id=0 should exist");
  }

  @Test
  void testImportFromFileWithSingleCrudAndStorageMode_ShouldSucceed() throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("import_data/import_single_trn_full.csv"))
                .toURI());
    Path controlFilePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("control_files/control_file_trn.json"))
                .toURI());
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "employee_trn",
      "--log-dir",
      tempDir.toString(),
      "--format",
      "CSV",
      "--mode",
      "STORAGE",
      "--import-mode",
      "UPSERT",
      "--file",
      filePath.toString(),
      "--control-file",
      controlFilePath.toString(),
      "--control-file-validation",
      "FULL",
      "--log-success"
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertThat(exitCode).isEqualTo(0);
  }

  @Test
  void testImportFromFileWithSingleCrudAndInsertMode_ShouldSucceed() throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("import_data/import_data_all.csv"))
                .toURI());
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      NAMESPACE,
      "--table",
      TABLE_ALL_COLUMNS,
      "--log-dir",
      tempDir.toString(),
      "--format",
      "CSV",
      "--import-mode",
      "INSERT",
      "--file",
      filePath.toString()
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertThat(exitCode).isEqualTo(0);
  }

  @Test
  void testImportFromFileWithSingleCrudAndUpdateMode_ShouldSucceed() throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("import_data/import_data_all.csv"))
                .toURI());
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      NAMESPACE,
      "--table",
      TABLE_ALL_COLUMNS,
      "--log-dir",
      tempDir.toString(),
      "--format",
      "CSV",
      "--import-mode",
      "UPDATE",
      "--file",
      filePath.toString()
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertThat(exitCode).isEqualTo(0);
  }

  @Test
  void testImportFromFileWithSingleCrudAndUpsertMode_ShouldSucceed() throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("import_data/import_data_all.csv"))
                .toURI());
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      NAMESPACE,
      "--table",
      TABLE_ALL_COLUMNS,
      "--log-dir",
      tempDir.toString(),
      "--format",
      "CSV",
      "--import-mode",
      "UPSERT",
      "--file",
      filePath.toString()
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertThat(exitCode).isEqualTo(0);
  }
}
