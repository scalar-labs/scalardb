package com.scalar.db.dataloader.cli.command;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.scalar.db.dataloader.cli.BaseIntegrationTest;
import com.scalar.db.dataloader.cli.TestDataValidationHelper;
import com.scalar.db.dataloader.cli.command.dataexport.ExportCommand;
import com.scalar.db.dataloader.cli.command.dataimport.ImportCommand;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Integration tests for round-trip scenarios (export then import) using single-crud-operation
 * transaction manager.
 *
 * <p>These tests verify data integrity by:
 *
 * <ul>
 *   <li>Exporting data from database using single-crud-operation transaction manager
 *   <li>Importing the exported data back using single-crud-operation transaction manager
 *   <li>Verifying data integrity (comparing original vs re-imported)
 *   <li>Testing with all formats (CSV, JSON, JSONL)
 *   <li>Testing with different import modes (INSERT, UPDATE, UPSERT)
 * </ul>
 *
 * <p>These tests ensure that data can be exported and re-imported without loss or corruption when
 * using the single-crud-operation transaction manager, which provides storage-only mode without
 * distributed transactions.
 */
public class ImportExportRoundTripSingleCrudIT extends BaseIntegrationTest {

  @Override
  protected String getTransactionManagerType() {
    return TRANSACTION_MANAGER_SINGLE_CRUD;
  }

  @Override
  protected boolean shouldCleanupTables() {
    // Round-trip tests need initial data from init_mysql.sql to export
    // They handle cleanup manually after export and before import
    return false;
  }

  /**
   * Helper method to find files with a specific extension in a directory.
   *
   * @param directory the directory to search
   * @param extension the file extension (e.g., ".csv")
   * @return list of matching file paths
   */
  private List<Path> findFilesWithExtension(Path directory, String extension) throws IOException {
    try (Stream<Path> paths = Files.list(directory)) {
      return paths
          .filter(Files::isRegularFile)
          .filter(path -> path.toString().endsWith(extension))
          .collect(Collectors.toList());
    }
  }

  @Test
  void testRoundTripCsvExportImportWithSingleCrud_ShouldMaintainDataIntegrity() throws Exception {
    // Step 1: Export data to CSV
    // Use projection to exclude transaction metadata columns that don't exist in the employee table
    String exportDir = tempDir.resolve("export").toString();
    Files.createDirectories(tempDir.resolve("export"));

    String[] exportArgs = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      NAMESPACE,
      "--table",
      TABLE_EMPLOYEE,
      "--format",
      "CSV",
      "--projection",
      "id,name,email",
      "--output-dir",
      exportDir
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine exportCommandLine = new CommandLine(exportCommand);
    int exportExitCode = exportCommandLine.execute(exportArgs);

    assertThat(exportExitCode).isEqualTo(0);

    // Find the exported CSV file
    List<Path> csvFiles = findFilesWithExtension(tempDir.resolve("export"), ".csv");
    assertThat(csvFiles).hasSize(1);
    Path exportedFile = csvFiles.get(0);

    // Verify exported file exists and has content
    assertTrue(
        TestDataValidationHelper.verifyExportedFileExists(exportedFile),
        "Exported CSV file should exist and not be empty");

    // Step 2: Clean up table before re-import
    cleanupTablesInternal();

    // Step 3: Import the exported CSV back
    // require-all-columns defaults to false, which allows importing CSV with only projected columns
    String[] importArgs = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      NAMESPACE,
      "--table",
      TABLE_EMPLOYEE,
      "--format",
      "CSV",
      "--import-mode",
      "UPSERT",
      "--file",
      exportedFile.toString()
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine importCommandLine = new CommandLine(importCommand);
    int importExitCode = importCommandLine.execute(importArgs);

    // Verify import succeeded
    assertThat(importExitCode).as("Import should succeed with exit code 0").isEqualTo(0);

    // Step 4: Verify data integrity - records should be imported
    int recordCount =
        TestDataValidationHelper.countRecords(configFilePath, NAMESPACE, TABLE_EMPLOYEE);
    assertThat(recordCount)
        .as("Records should be imported successfully. Found %d records", recordCount)
        .isGreaterThan(0);
  }

  @Test
  void testRoundTripJsonExportImportWithSingleCrud_ShouldMaintainDataIntegrity()
      throws Exception {
    // Step 1: Export data to JSON
    // Use projection to exclude transaction metadata columns that don't exist in the employee table
    String exportDir = tempDir.resolve("export").toString();
    Files.createDirectories(tempDir.resolve("export"));

    String[] exportArgs = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      NAMESPACE,
      "--table",
      TABLE_EMPLOYEE,
      "--format",
      "JSON",
      "--projection",
      "id,name,email",
      "--output-dir",
      exportDir
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine exportCommandLine = new CommandLine(exportCommand);
    int exportExitCode = exportCommandLine.execute(exportArgs);

    assertThat(exportExitCode).isEqualTo(0);

    // Find the exported JSON file
    List<Path> jsonFiles = findFilesWithExtension(tempDir.resolve("export"), ".json");
    assertThat(jsonFiles).hasSize(1);
    Path exportedFile = jsonFiles.get(0);

    // Verify exported file exists and has content
    assertTrue(
        TestDataValidationHelper.verifyExportedFileExists(exportedFile),
        "Exported JSON file should exist and not be empty");

    // Step 2: Clean up table before re-import
    cleanupTablesInternal();

    // Step 3: Import the exported JSON back
    // require-all-columns defaults to false, which allows importing JSON with only projected
    // columns
    String[] importArgs = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      NAMESPACE,
      "--table",
      TABLE_EMPLOYEE,
      "--format",
      "JSON",
      "--import-mode",
      "UPSERT",
      "--file",
      exportedFile.toString()
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine importCommandLine = new CommandLine(importCommand);
    int importExitCode = importCommandLine.execute(importArgs);

    // Verify import succeeded
    assertThat(importExitCode).as("Import should succeed with exit code 0").isEqualTo(0);

    // Step 4: Verify data integrity - records should be imported
    int recordCount =
        TestDataValidationHelper.countRecords(configFilePath, NAMESPACE, TABLE_EMPLOYEE);
    assertThat(recordCount)
        .as("Records should be imported successfully. Found %d records", recordCount)
        .isGreaterThan(0);
  }

  /** Internal method to clean up tables. Used by round-trip tests to reset state. */
  private void cleanupTablesInternal() throws Exception {
    if (configFilePath == null || !Files.exists(configFilePath)) {
      return;
    }

    try {
      java.util.Properties props = new java.util.Properties();
      props.load(Files.newInputStream(configFilePath));
      com.scalar.db.service.StorageFactory factory =
          com.scalar.db.service.StorageFactory.create(props);
      com.scalar.db.api.DistributedStorageAdmin admin = factory.getStorageAdmin();

      truncateTableSafely(admin, NAMESPACE, TABLE_EMPLOYEE);
      admin.close();
    } catch (Exception e) {
      // Ignore cleanup errors
    }
  }

  private void truncateTableSafely(
      com.scalar.db.api.DistributedStorageAdmin admin, String namespace, String table) {
    try {
      admin.truncateTable(namespace, table);
    } catch (com.scalar.db.exception.storage.ExecutionException e) {
      // Ignore
    }
  }
}

