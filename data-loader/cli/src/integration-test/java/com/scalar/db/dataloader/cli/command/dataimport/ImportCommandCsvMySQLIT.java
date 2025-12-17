package com.scalar.db.dataloader.cli.command.dataimport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.scalar.db.dataloader.cli.BaseIntegrationTest;
import com.scalar.db.dataloader.cli.TestDataValidationHelper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.MountableFile;
import picocli.CommandLine;

/**
 * Integration tests for CSV import functionality using MySQL.
 *
 * <p>Tests various CSV import scenarios including:
 *
 * <ul>
 *   <li>Different import modes (INSERT, UPDATE, UPSERT)
 *   <li>Transaction and storage modes
 *   <li>Control file validation (FULL, MAPPED)
 *   <li>Custom delimiters and headers
 *   <li>Logging options
 * </ul>
 *
 * <p>These tests use a shared MySQL container initialized with test data. Each test starts with a
 * clean state (tables are truncated before each test) to ensure test isolation.
 *
 * @see BaseIntegrationTest for shared test infrastructure
 */
public class ImportCommandCsvMySQLIT extends BaseIntegrationTest {

  @BeforeAll
  static void setupContainer() {
    // Copy initialization SQL file to container
    mysql.withCopyFileToContainer(
        MountableFile.forClasspathResource("init_mysql_import.sql"),
        "/docker-entrypoint-initdb.d/init_mysql.sql");
  }

  @Test
  void testImportFromFileWithTransactionModeAndDataChunkSizeAndTransactionSizeWithFormatCSV()
      throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("import_data/import_single_mapped.csv"))
                .toURI());
    Path controlFilePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("control_files/control_file_trn_mapped.json"))
                .toURI());
    Path logPath = tempDir.resolve("logs");
    Files.createDirectories(logPath);
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "employee_trn",
      "--log-dir",
      logPath.toString(),
      "--format",
      "CSV",
      "--import-mode",
      "UPSERT",
      "--mode",
      "TRANSACTION",
      "--file",
      filePath.toString(),
      "--control-file",
      controlFilePath.toString(),
      "--control-file-validation",
      "MAPPED",
      "--log-raw-record",
      "--log-success",
      "--transaction-size",
      "1",
      "--data-chunk-size",
      "5"
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);
    assertThat(exitCode).isEqualTo(0);
  }

  @Test
  void testImportFromFileWithTransactionModeWithFormatCSVAndSplitLogMode() throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("import_data/import_single_mapped.csv"))
                .toURI());
    Path controlFilePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("control_files/control_file_trn_mapped.json"))
                .toURI());
    Path logPath = tempDir.resolve("logs");
    Files.createDirectories(logPath);
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "employee_trn",
      "--log-dir",
      logPath.toString(),
      "--format",
      "CSV",
      "--import-mode",
      "UPSERT",
      "--mode",
      "TRANSACTION",
      "--file",
      filePath.toString(),
      "--control-file",
      controlFilePath.toString(),
      "--control-file-validation",
      "MAPPED",
      "--log-raw-record",
      "--log-success",
      "--split-log-mode"
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    assertThat(commandLine.execute(args)).isEqualTo(0);
  }

  @Test
  void
      testImportFromFileWithImportModeUpsertWithControlFileWithStorageModeAndFileFormatCSVWithTwoTables()
          throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("import_data/import_multi_mapped.csv"))
                .toURI());
    Path controlFilePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("control_files/control_file_multi.json"))
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
  void testImportFromFileWithImportModeInsertAndFileFormatCSV() throws Exception {
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
  void testImportFromFileWithImportModeUpdateAndFileFormatCSV() throws Exception {
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
  void testImportFromFileWithImportModeUpsertAndFileFormatCSV() throws Exception {
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

  @Test
  void testImportFromFileWithControlFileWithStorageModeAndFileFormatCSV() throws Exception {
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
      "TRANSACTION",
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
  void testImportFromFileWithStorageModeTransactionAndFileFormatCSV_WithLogRawRecordsEnabled()
      throws Exception {
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
    Path logPath = tempDir.resolve("logs");
    Files.createDirectories(logPath);
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "employee_trn",
      "--log-dir",
      logPath.toString(),
      "--format",
      "CSV",
      "--import-mode",
      "UPSERT",
      "--mode",
      "TRANSACTION",
      "--file",
      filePath.toString(),
      "--control-file",
      controlFilePath.toString(),
      "--control-file-validation",
      "MAPPED",
      "--log-raw-record",
      "--log-success",
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);
    assertThat(exitCode).isEqualTo(0);
  }

  @Test
  void testImportFromFileWithImportModeUpsertAndFileFormatCsv() throws Exception {
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
  void testImportFromFileWithImportModeUpdateAndFileFormatCsv() throws Exception {
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
  void testImportFromFileWithImportModeUpsertAndFileFormatCsvWithoutHeader() throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("import_data/import_single_without_header.csv"))
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
      filePath.toString(),
      "--delimiter",
      ",",
      "--header",
      "id,name,email"
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertThat(exitCode).isEqualTo(0);
  }

  @Test
  void testImportFromFileWithImportModeUpdateAndFileFormatCsvWithoutHeader() throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("import_data/import_single_without_header.csv"))
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
      "UPDATE",
      "--file",
      filePath.toString(),
      "--delimiter",
      ",",
      "--header",
      "id,name,email"
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertThat(exitCode).isEqualTo(0);
  }

  @Test
  void testImportFromFileWithImportModeUpsertAndFileFormatCsvWithDifferentDelimiter()
      throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("import_data/import_single_delimiter.csv"))
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
      filePath.toString(),
      "--delimiter",
      ":",
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertThat(exitCode).isEqualTo(0);
  }
}
