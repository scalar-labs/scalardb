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
 * Integration tests for JSON import functionality using MySQL.
 *
 * <p>Tests various JSON import scenarios including:
 *
 * <ul>
 *   <li>Different import modes (INSERT, UPDATE, UPSERT)
 *   <li>Transaction mode with control files
 *   <li>JSON-specific options (pretty-print, require-all-columns, ignore-nulls)
 *   <li>Control file validation
 * </ul>
 *
 * <p>These tests use a shared MySQL container initialized with test data. Each test starts with a
 * clean state (tables are truncated before each test) to ensure test isolation.
 *
 * @see BaseIntegrationTest for shared test infrastructure
 */
public class ImportCommandJsonMySQLIT extends BaseIntegrationTest {

  @BeforeAll
  static void setupContainer() {
    // Copy initialization SQL file to container
    mysql.withCopyFileToContainer(
        MountableFile.forClasspathResource("init_mysql_import.sql"),
        "/docker-entrypoint-initdb.d/init_mysql.sql");
  }

  @Test
  void testImportFromFileWithImportModeUpsertAndFileFormatJsonWithAllColumns() throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("import_data/import_single.json"))
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
      "JSON",
      "--import-mode",
      "UPSERT",
      "--file",
      filePath.toString(),
      "--require-all-columns"
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertThat(exitCode).isEqualTo(0);
  }

  @Test
  void testImportFromFileWithImportModeUpsertAndFileFormatJsonWithPrettyPrints() throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("import_data/import_single.json"))
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
      "JSON",
      "--import-mode",
      "UPSERT",
      "--file",
      filePath.toString(),
      "--pretty-print"
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertThat(exitCode).isEqualTo(0);
  }

  @Test
  void testImportFromFileAndFileFormatJsonWithIgnoreNulls() throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("import_data/import_data_with_null.json"))
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
      "JSON",
      "--import-mode",
      "UPSERT",
      "--file",
      filePath.toString(),
      "--ignore-nulls"
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertThat(exitCode).isEqualTo(0);
  }

  @Test
  void testImportFromFileWithImportModeUpsertAndFileFormatJson() throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("import_data/import_single.json"))
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
      "JSON",
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
  void testImportFromFileWithImportModeUpdateAndFileFormatJson() throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("import_data/import_single.json"))
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
      "JSON",
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
  void
      testImportFromFileWithImportModeUpsertWithControlFileWithStorageModeTransactionAndFileFormatJSON()
          throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("import_data/import_single_trn_full.json"))
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
      "JSON",
      "--mode",
      "TRANSACTION",
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
  void
      testImportFromFileWithImportModeUpsertWithControlFileWithStorageModeAndFileFormatJSONWithTwoTables()
          throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("import_data/import_multi_mapped.json"))
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
      "JSON",
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
  void testImportFromFileWithTransactionModeAndDataChunkSizeAndTransactionSizeWithFormatJson()
      throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("import_data/import_single_mapped.json"))
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
      "JSON",
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
