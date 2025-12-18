package com.scalar.db.dataloader.cli.command.dataimport;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.dataloader.cli.BaseIntegrationTest;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Integration tests for negative scenarios in data import functionality.
 *
 * <p>These tests verify that the import command handles error cases gracefully:
 *
 * <ul>
 *   <li>Invalid file paths (non-existent files)
 *   <li>Invalid file formats
 *   <li>Invalid command-line arguments
 *   <li>Invalid table/namespace names
 *   <li>Missing required parameters
 * </ul>
 */
public class ImportCommandNegativeIT extends BaseIntegrationTest {

  @Test
  void testImportWithNonExistentFile_ShouldFail() throws Exception {
    String[] args = {
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
      "/non/existent/file.csv"
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertThat(exitCode).isNotEqualTo(0);
  }

  @Test
  void testImportWithInvalidNamespace_ShouldFail() throws Exception {
    Path filePath = tempDir.resolve("test.csv");
    Files.write(filePath, "id,name,email\n1,test,test@example.com".getBytes());

    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "nonexistent_namespace",
      "--table",
      TABLE_EMPLOYEE,
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

    assertThat(exitCode).isNotEqualTo(0);
  }

  @Test
  void testImportWithInvalidTable_ShouldFail() throws Exception {
    Path filePath = tempDir.resolve("test.csv");
    Files.write(filePath, "id,name,email\n1,test,test@example.com".getBytes());

    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      NAMESPACE,
      "--table",
      "nonexistent_table",
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
    assertThat(exitCode).isNotEqualTo(0);
  }

  @Test
  void testImportWithMissingRequiredArguments_ShouldFail() throws Exception {
    Path filePath = tempDir.resolve("test.csv");
    Files.write(filePath, "id,name,email\n1,test,test@example.com".getBytes());

    // Missing --namespace
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--table",
      TABLE_EMPLOYEE,
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

    assertThat(exitCode).isNotEqualTo(0);
  }

  @Test
  void testImportWithInvalidFormat_ShouldFail() throws Exception {
    Path filePath = tempDir.resolve("test.xyz");
    Files.write(filePath, "some content".getBytes());

    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      NAMESPACE,
      "--table",
      TABLE_EMPLOYEE,
      "--format",
      "INVALID_FORMAT",
      "--import-mode",
      "UPSERT",
      "--file",
      filePath.toString()
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertThat(exitCode).isNotEqualTo(0);
  }

  @Test
  void testImportWithInvalidImportMode_ShouldFail() throws Exception {
    Path filePath = tempDir.resolve("test.csv");
    Files.write(filePath, "id,name,email\n1,test,test@example.com".getBytes());

    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      NAMESPACE,
      "--table",
      TABLE_EMPLOYEE,
      "--format",
      "CSV",
      "--import-mode",
      "INVALID_MODE",
      "--file",
      filePath.toString()
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertThat(exitCode).isNotEqualTo(0);
  }

  @Test
  void testImportWithEmptyFile_ShouldHandleGracefully() throws Exception {
    Path filePath = tempDir.resolve("empty.csv");
    Files.createFile(filePath); // Create empty file

    String[] args = {
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
      filePath.toString()
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    // Empty file causes failure in CSV parser (null header)
    // This test documents the current behavior - empty files fail
    assertThat(exitCode).isNotEqualTo(0);
  }

  @Test
  void testImportWithInvalidConfigFile_ShouldFail() throws Exception {
    Path invalidConfigFile = tempDir.resolve("invalid.properties");
    Files.write(invalidConfigFile, "invalid.config=value".getBytes());

    Path filePath = tempDir.resolve("test.csv");
    Files.write(filePath, "id,name,email\n1,test,test@example.com".getBytes());

    String[] args = {
      "--config",
      invalidConfigFile.toString(),
      "--namespace",
      NAMESPACE,
      "--table",
      TABLE_EMPLOYEE,
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

    assertThat(exitCode).isNotEqualTo(0);
  }

  @Test
  void testImportWithNegativeTransactionSize_ShouldFail() throws Exception {
    Path filePath = tempDir.resolve("test.csv");
    Files.write(filePath, "id,name,email\n1,test,test@example.com".getBytes());

    String[] args = {
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
      "--transaction-size",
      "-1",
      "--file",
      filePath.toString()
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertThat(exitCode).isNotEqualTo(0);
  }

  @Test
  void testImportWithZeroDataChunkSize_ShouldFail() throws Exception {
    Path filePath = tempDir.resolve("test.csv");
    Files.write(filePath, "id,name,email\n1,test,test@example.com".getBytes());

    String[] args = {
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
      "--data-chunk-size",
      "0",
      "--file",
      filePath.toString()
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertThat(exitCode).isNotEqualTo(0);
  }
}
