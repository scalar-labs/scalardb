package com.scalar.db.dataloader.cli.command.dataexport;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.dataloader.cli.BaseIntegrationTest;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Integration tests for negative scenarios in data export functionality.
 *
 * <p>These tests verify that the export command handles error cases gracefully:
 *
 * <ul>
 *   <li>Invalid table/namespace names
 *   <li>Invalid command-line arguments
 *   <li>Missing required parameters
 *   <li>Invalid output directory
 * </ul>
 */
public class ExportCommandNegativeIT extends BaseIntegrationTest {

  @Override
  protected boolean shouldCleanupTables() {
    // Export tests need data to export
    return false;
  }

  @Test
  void testExportWithInvalidNamespace_ShouldFail() throws Exception {
    String outputDir = tempDir.toString();
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "nonexistent_namespace",
      "--table",
      TABLE_ALL_COLUMNS,
      "--output-dir",
      outputDir
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);

    assertThat(exitCode).isNotEqualTo(0);
  }

  @Test
  void testExportWithInvalidTable_ShouldFail() throws Exception {
    String outputDir = tempDir.toString();
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      NAMESPACE,
      "--table",
      "nonexistent_table",
      "--format",
      "JSON",
      "--output-dir",
      outputDir
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);

    assertThat(exitCode).isNotEqualTo(0);
  }

  @Test
  void testExportWithMissingRequiredArguments_ShouldFail() throws Exception {
    String outputDir = tempDir.toString();
    // Missing --namespace
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--table",
      TABLE_ALL_COLUMNS,
      "--format",
      "JSON",
      "--output-dir",
      outputDir
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);

    assertThat(exitCode).isNotEqualTo(0);
  }

  @Test
  void testExportWithInvalidFormat_ShouldFail() throws Exception {
    String outputDir = tempDir.toString();
    String[] args = {
      "--config", configFilePath.toString(),
      "--namespace", NAMESPACE,
      "--table", TABLE_ALL_COLUMNS,
      "--format", "INVALID_FORMAT",
      "--output-dir", outputDir
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);

    assertThat(exitCode).isNotEqualTo(0);
  }

  @Test
  void testExportWithInvalidConfigFile_ShouldFail() throws Exception {
    Path invalidConfigFile = tempDir.resolve("invalid.properties");
    java.nio.file.Files.write(invalidConfigFile, "invalid.config=value".getBytes());

    String outputDir = tempDir.toString();
    String[] args = {
      "--config", invalidConfigFile.toString(),
      "--namespace", NAMESPACE,
      "--table", TABLE_ALL_COLUMNS,
      "--format", "JSON",
      "--output-dir", outputDir
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);

    assertThat(exitCode).isNotEqualTo(0);
  }

  @Test
  void testExportWithNegativeLimit_CurrentBehavior() throws Exception {
    // Note: Current CLI does not validate negative limit values
    // This test documents the current behavior - negative limit is accepted
    String outputDir = tempDir.toString();
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      NAMESPACE,
      "--table",
      TABLE_ALL_COLUMNS,
      "--format",
      "JSON",
      "--limit",
      "-1",
      "--output-dir",
      outputDir
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);

    // Current behavior: negative limit is accepted and export succeeds
    // TODO: Consider adding validation for negative limit values in CLI
    assertThat(exitCode).isEqualTo(0);
  }
}
