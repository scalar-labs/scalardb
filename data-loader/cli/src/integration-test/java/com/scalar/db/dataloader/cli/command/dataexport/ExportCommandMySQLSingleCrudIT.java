package com.scalar.db.dataloader.cli.command.dataexport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.scalar.db.dataloader.cli.BaseIntegrationTest;
import com.scalar.db.dataloader.cli.TestDataValidationHelper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Integration tests for data export functionality using MySQL with single-crud-operation
 * transaction manager.
 *
 * <p>These tests verify that data export operations work correctly when using the
 * single-crud-operation transaction manager. This transaction manager provides storage-only mode
 * without distributed transactions, suitable for simple CRUD operations.
 *
 * <p>Tests various export scenarios including:
 *
 * <ul>
 *   <li>Different output formats (CSV, JSON, JSONL)
 *   <li>Partition key filtering
 *   <li>Scan ranges (start-key, end-key)
 *   <li>Metadata inclusion
 *   <li>Custom delimiters and headers
 * </ul>
 *
 * <p>These tests use a shared MySQL container initialized with test data. Unlike import tests,
 * export tests preserve data between tests (cleanup is disabled) since they need data to export.
 *
 * @see BaseIntegrationTest for shared test infrastructure
 */
public class ExportCommandMySQLSingleCrudIT extends BaseIntegrationTest {

  @Override
  protected String getTransactionManagerType() {
    return TRANSACTION_MANAGER_SINGLE_CRUD;
  }

  @Override
  protected boolean shouldCleanupTables() {
    // Export tests need data to export, so don't clean up tables
    return false;
  }

  private static final String CSV_EXTENSION = ".csv";
  private static final String JSON_EXTENSION = ".json";
  private static final String JSONLINES_EXTENSION = ".jsonl";

  @Test
  void testExportToFileWithSingleCrudTransactionManager_ShouldSucceed() throws IOException {
    String outputDir = tempDir.toString();
    String[] args = {
      "--config", configFilePath.toString(),
      "--namespace", NAMESPACE,
      "--table", TABLE_ALL_COLUMNS,
      "--output-dir", outputDir,
      "--format", "CSV"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertThat(exitCode).isEqualTo(0);

    // Verify output file
    List<Path> files = findFilesWithExtension(tempDir, CSV_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(CSV_EXTENSION);
    // Verify exported file exists and has content
    assertTrue(
        TestDataValidationHelper.verifyExportedFileExists(files.get(0)),
        "Exported file should exist and have content");
  }

  @Test
  void testExportToFileWithSingleCrudAndJSONFormat_ShouldSucceed() throws IOException {
    String outputDir = tempDir.toString();

    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      NAMESPACE,
      "--table",
      TABLE_ALL_COLUMNS,
      "--output-dir",
      outputDir,
      "--format",
      "JSON",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertThat(exitCode).isEqualTo(0);
    List<Path> files = findFilesWithExtension(tempDir, JSON_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(JSON_EXTENSION);
  }

  @Test
  void testExportToFileWithSingleCrudAndJSONLinesFormat_ShouldSucceed() throws IOException {
    String outputDir = tempDir.toString();
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      NAMESPACE,
      "--table",
      TABLE_ALL_COLUMNS,
      "--output-dir",
      outputDir,
      "--format",
      "JSONL",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertThat(exitCode).isEqualTo(0);
    List<Path> files = findFilesWithExtension(tempDir, JSONLINES_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(JSONLINES_EXTENSION);
  }

  public static List<Path> findFilesWithExtension(Path directory, String extension)
      throws IOException {
    try (Stream<Path> files = Files.list(directory)) {
      return files.filter(path -> path.toString().endsWith(extension)).collect(Collectors.toList());
    }
  }
}
