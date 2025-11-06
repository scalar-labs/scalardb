package com.scalar.db.dataloader.cli.command.dataimport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.dataloader.core.dataimport.ImportMode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

public class ImportCommandTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ImportCommandTest.class);
  @TempDir Path tempDir;

  private ImportCommand importCommand;

  @BeforeEach
  void setUp() {
    importCommand = new ImportCommand();
    CommandLine cmd = new CommandLine(importCommand);
    // Parse empty args to properly initialize spec.commandLine() reference
    try {
      cmd.parseArgs();
    } catch (Exception e) {
      // Ignore parse errors for missing required options - we'll set fields directly in tests
    }
    importCommand.spec = cmd.getCommandSpec();
  }

  @AfterEach
  public void cleanup() throws IOException {
    cleanUpTempDir();
  }

  @Test
  void call_WithoutValidConfigFile_ShouldThrowException() throws Exception {
    Path configFile = tempDir.resolve("config.properties");
    Files.createFile(configFile);
    Path importFile = tempDir.resolve("import.json");
    Files.createFile(importFile);
    importCommand.configFilePath = configFile.toString();
    importCommand.namespace = "sample";
    importCommand.tableName = "table";
    importCommand.sourceFileFormat = FileFormat.JSON;
    importCommand.sourceFilePath = importFile.toString();
    importCommand.importMode = ImportMode.UPSERT;
    importCommand.dataChunkSize = 100;
    importCommand.transactionSize = 10;
    importCommand.threadCount = 4;
    importCommand.dataChunkQueueSize = 64;
    assertThrows(IllegalArgumentException.class, () -> importCommand.call());
  }

  private void cleanUpTempDir() throws IOException {
    try (Stream<Path> paths = Files.list(tempDir)) {
      paths.forEach(this::deleteFile);
    }
  }

  private void deleteFile(Path file) {
    try {
      Files.deleteIfExists(file);
    } catch (IOException e) {
      LOGGER.error("Failed to delete file: {}", file, e);
    }
  }

  @Test
  void call_withBothThreadsAndMaxThreads_shouldThrowException() throws Exception {
    Path configFile = tempDir.resolve("config.properties");
    Files.createFile(configFile);
    Path importFile = tempDir.resolve("import.json");
    Files.createFile(importFile);

    // Simulate command line parsing with both deprecated and new options
    String[] args = {
      "--config",
      configFile.toString(),
      "--file",
      importFile.toString(),
      "--namespace",
      "sample",
      "--table",
      "table",
      "--max-threads",
      "8",
      "--threads",
      "16"
    };
    ImportCommand command = new ImportCommand();
    CommandLine cmd = new CommandLine(command);
    // Parse args - this will trigger our validation
    cmd.parseArgs(args);

    // Now call the command, which should throw the validation error
    CommandLine.ParameterException thrown =
        assertThrows(
            CommandLine.ParameterException.class,
            command::call,
            "Expected to throw ParameterException when both deprecated and new options are specified");
    assertTrue(
        thrown
            .getMessage()
            .contains(
                "Cannot specify both deprecated option '--max-threads' and new option '--threads'"));
  }

  @Test
  void call_withOnlyDeprecatedMaxThreads_shouldApplyValue() throws Exception {
    Path configFile = tempDir.resolve("config.properties");
    Files.createFile(configFile);
    Path importFile = tempDir.resolve("import.json");
    Files.createFile(importFile);

    // Simulate command line parsing with only deprecated option
    String[] args = {
      "--config",
      configFile.toString(),
      "--file",
      importFile.toString(),
      "--namespace",
      "sample",
      "--table",
      "table",
      "--max-threads",
      "12"
    };
    ImportCommand command = new ImportCommand();
    CommandLine cmd = new CommandLine(command);
    cmd.parseArgs(args);

    // Verify the deprecated value was parsed
    assertEquals(12, command.maxThreadsDeprecated);

    // Apply deprecated options (this is what the command does after validation)
    command.applyDeprecatedOptions();

    // Verify the value was applied to threadCount
    assertEquals(12, command.threadCount);
  }

  @Test
  void call_withOnlyThreads_shouldUseValue() throws Exception {
    Path configFile = tempDir.resolve("config.properties");
    Files.createFile(configFile);
    Path importFile = tempDir.resolve("import.json");
    Files.createFile(importFile);

    // Simulate command line parsing with only new --threads option
    String[] args = {
      "--config",
      configFile.toString(),
      "--file",
      importFile.toString(),
      "--namespace",
      "sample",
      "--table",
      "table",
      "--threads",
      "20"
    };
    ImportCommand command = new ImportCommand();
    CommandLine cmd = new CommandLine(command);
    cmd.parseArgs(args);

    // Verify the value was set to threadCount
    assertEquals(20, command.threadCount);

    // Verify the deprecated value was not set
    assertEquals(null, command.maxThreadsDeprecated);
  }

  @Test
  void call_withDeprecatedShortOption_shouldApplyValue() throws Exception {
    Path configFile = tempDir.resolve("config.properties");
    Files.createFile(configFile);
    Path importFile = tempDir.resolve("import.json");
    Files.createFile(importFile);

    // Simulate command line parsing with deprecated short option -mt
    String[] args = {
      "--config",
      configFile.toString(),
      "--file",
      importFile.toString(),
      "--namespace",
      "sample",
      "--table",
      "table",
      "-mt",
      "15"
    };
    ImportCommand command = new ImportCommand();
    CommandLine cmd = new CommandLine(command);
    cmd.parseArgs(args);

    // Verify the deprecated value was parsed
    assertEquals(15, command.maxThreadsDeprecated);

    // Apply deprecated options
    command.applyDeprecatedOptions();

    // Verify the value was applied to threadCount
    assertEquals(15, command.threadCount);
  }
}
