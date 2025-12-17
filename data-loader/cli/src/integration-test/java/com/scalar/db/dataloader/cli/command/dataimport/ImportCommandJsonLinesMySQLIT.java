package com.scalar.db.dataloader.cli.command.dataimport;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.dataloader.cli.BaseIntegrationTest;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.MountableFile;
import picocli.CommandLine;

/**
 * Integration tests for JSON Lines (JSONL) import functionality using MySQL.
 *
 * <p>Tests various JSONL import scenarios including:
 *
 * <ul>
 *   <li>Different import modes (INSERT, UPDATE, UPSERT)
 *   <li>Transaction mode with control files
 *   <li>Multi-table imports
 *   <li>Threading options (max-threads)
 * </ul>
 *
 * <p>JSON Lines format is a text format where each line is a valid JSON object, making it suitable
 * for streaming large datasets.
 *
 * <p>These tests use a shared MySQL container initialized with test data. Each test starts with a
 * clean state (tables are truncated before each test) to ensure test isolation.
 *
 * @see BaseIntegrationTest for shared test infrastructure
 */
public class ImportCommandJsonLinesMySQLIT extends BaseIntegrationTest {

  @BeforeAll
  static void setupContainer() {
    // Copy initialization SQL file to container
    mysql.withCopyFileToContainer(
        MountableFile.forClasspathResource("init_mysql_import.sql"),
        "/docker-entrypoint-initdb.d/init_mysql.sql");
  }

  @Test
  void
      testImportFromFileWithImportModeUpsertWithControlFileWithStorageModeTransactionAndFileFormatJSONLines()
          throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("import_data/import_single_trn_full.jsonl"))
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
      NAMESPACE,
      "--table",
      TABLE_EMPLOYEE_TRN,
      "--log-dir",
      tempDir.toString(),
      "--format",
      "JSONL",
      "--import-mode",
      "UPSERT",
      "--mode",
      "TRANSACTION",
      "--file",
      filePath.toString(),
      "--control-file",
      controlFilePath.toString(),
      "--control-file-validation",
      "FULL"
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertThat(exitCode).isEqualTo(0);
  }

  @Test
  void testImportFromFileWithRequiredInputsOnlyAndFileFormatJsonLines() throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("import_data/import_single.jsonl"))
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
      "JSONL",
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
  void testImportFromFileWithImportModeUpsertWithControlFile() throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("import_data/import_single_mapped.jsonl"))
                .toURI());
    Path controlFilePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("control_files/control_file.json"))
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
      "JSONL",
      "--import-mode",
      "UPSERT",
      "--file",
      filePath.toString(),
      "--control-file",
      controlFilePath.toString(),
      "--control-file-validation",
      "FULL"
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertThat(exitCode).isEqualTo(0);
  }

  @Test
  void testImportFromFileWithRequiredInputsOnlyAndFileFormatJsonLinesWithMaxThreads()
      throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("import_data/import_single.jsonl"))
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
      "JSONL",
      "--import-mode",
      "INSERT",
      "--file",
      filePath.toString(),
      "--max-threads",
      "4"
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertThat(exitCode).isEqualTo(0);
  }

  @Test
  void testImportFromFileAndFileFormatJsonLines() throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("import_data/import_single.jsonl"))
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
      "JSONL",
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
  void testImportFromFileWithImportModeUpsertAndFileFormatJsonLines() throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("import_data/import_single.jsonl"))
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
      "JSONL",
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
  void
      testImportFromFileWithImportModeUpsertWithControlFileMappedWithStorageModeTransactionAndFileFormatCSV_WithLogRawRecordsEnabled()
          throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("import_data/import_single_mapped.jsonl"))
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
      NAMESPACE,
      "--table",
      TABLE_EMPLOYEE_TRN,
      "--log-dir",
      logPath.toString(),
      "--format",
      "JSONL",
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
      "--log-success"
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);
    assertThat(exitCode).isEqualTo(0);
  }

  @Test
  void
      testImportFromFileWithImportModeUpsertWithControlFileWithStorageModeAndFileFormatJSONLinesWithTwoTables()
          throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("import_data/import_multi_mapped.jsonl"))
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
      "test",
      "--table",
      "employee",
      "--log-dir",
      tempDir.toString(),
      "--format",
      "JSONL",
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
  void testImportFromFileWithTransactionModeAndDataChunkSizeAndTransactionSizeWithFormatJSONLines()
      throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("import_data/import_single_mapped.jsonl"))
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
      NAMESPACE,
      "--table",
      TABLE_EMPLOYEE_TRN,
      "--log-dir",
      logPath.toString(),
      "--format",
      "JSONL",
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
}
