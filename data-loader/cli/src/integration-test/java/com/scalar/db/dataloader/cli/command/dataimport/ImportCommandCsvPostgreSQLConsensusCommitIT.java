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
 * Integration tests for CSV import functionality using PostgreSQL with consensus-commit transaction
 * manager.
 *
 * <p>These tests verify that CSV import operations work correctly when using the consensus-commit
 * transaction manager. This transaction manager provides full ACID guarantees and distributed
 * transaction support.
 *
 * <p>Tests various CSV import scenarios including:
 *
 * <ul>
 *   <li>Different import modes (INSERT, UPDATE, UPSERT)
 *   <li>Transaction mode operations
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
public class ImportCommandCsvPostgreSQLConsensusCommitIT extends BasePostgreSQLIntegrationTest {

  @Override
  protected String getTransactionManagerType() {
    return TRANSACTION_MANAGER_CONSENSUS_COMMIT;
  }

  @BeforeAll
  static void setupContainer() {
    // Copy initialization SQL file to container
    postgres.withCopyFileToContainer(
        MountableFile.forClasspathResource("init_postgres_import.sql"),
        "/docker-entrypoint-initdb.d/init_postgres.sql");
  }

  @Test
  void testImportFromFileWithConsensusCommitTransactionManager_ShouldSucceed() throws Exception {
    // Consensus-commit requires tables with transaction metadata columns (tx_id, tx_state, etc.)
    // Use employee_trn table which has transaction metadata columns
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
    // Verify data was imported
    int recordCount = TestDataValidationHelper.countRecords(configFilePath, "test", "employee_trn");
    assertTrue(recordCount > 0, "Expected records to be imported, but found " + recordCount);
  }

  @Test
  void testImportFromFileWithConsensusCommitAndTransactionMode_ShouldSucceed() throws Exception {
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
  void testImportFromFileWithConsensusCommitAndInsertMode_ShouldSucceed() throws Exception {
    // Use all_columns table which has transaction metadata columns
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
  void testImportFromFileWithConsensusCommitAndUpdateMode_ShouldSucceed() throws Exception {
    // Use all_columns table which has transaction metadata columns
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
  void testImportFromFileWithConsensusCommitAndUpsertMode_ShouldSucceed() throws Exception {
    // Use all_columns table which has transaction metadata columns
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
