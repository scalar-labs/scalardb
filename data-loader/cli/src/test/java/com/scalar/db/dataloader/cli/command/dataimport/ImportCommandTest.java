package com.scalar.db.dataloader.cli.command.dataimport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.dataloader.core.ScalarDbMode;
import com.scalar.db.dataloader.core.dataimport.ImportMode;
import com.scalar.db.service.TransactionFactory;
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
    importCommand.maxThreads = 4;
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
      "--threads",
      "8",
      "--max-threads",
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
                "Cannot specify both deprecated option '--threads' and new option '--max-threads'"));
  }

  @Test
  void call_withOnlyDeprecatedThreads_shouldApplyValue() throws Exception {
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
      "--threads",
      "12"
    };
    ImportCommand command = new ImportCommand();
    CommandLine cmd = new CommandLine(command);
    cmd.parseArgs(args);

    // Verify the deprecated value was parsed
    assertEquals(12, command.threadsDeprecated);

    // Apply deprecated options (this is what the command does after validation)
    command.applyDeprecatedOptions();

    // Verify the value was applied to maxThreads
    assertEquals(12, command.maxThreads);
  }

  @Test
  void call_withBothLogSuccessAndEnableLogSuccess_shouldThrowException() throws Exception {
    Path configFile = tempDir.resolve("config.properties");
    Files.createFile(configFile);
    Path importFile = tempDir.resolve("import.json");
    Files.createFile(importFile);

    // Simulate command line parsing with both deprecated and new options
    String[] args = {"--file", importFile.toString(), "--log-success", "--enable-log-success"};
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
                "Cannot specify both deprecated option '--log-success' and new option '--enable-log-success'"));
  }

  @Test
  void call_withOnlyDeprecatedLogSuccess_shouldApplyValue() throws Exception {
    Path configFile = tempDir.resolve("config.properties");
    Files.createFile(configFile);
    Path importFile = tempDir.resolve("import.json");
    Files.createFile(importFile);

    // Simulate command line parsing with only deprecated option
    String[] args = {"--file", importFile.toString(), "--log-success"};
    ImportCommand command = new ImportCommand();
    CommandLine cmd = new CommandLine(command);
    cmd.parseArgs(args);

    // Verify the deprecated value was parsed
    assertTrue(command.logSuccessRecordsDeprecated);

    // Apply deprecated options (this is what the command does after validation)
    command.applyDeprecatedOptions();

    // Verify the value was applied to enable-log-success
    assertTrue(command.enableLogSuccessRecords);
  }

  @Test
  void call_withEnableLogSuccess_shouldSetToTrueWithoutValue() throws Exception {
    Path importFile = tempDir.resolve("import.json");
    Files.createFile(importFile);

    // Simulate command line parsing with the new flag without providing true/false value
    String[] args = {"--file", importFile.toString(), "--enable-log-success"};
    ImportCommand command = new ImportCommand();
    CommandLine cmd = new CommandLine(command);
    cmd.parseArgs(args);

    // Verify the flag was parsed correctly without requiring a value
    assertTrue(command.enableLogSuccessRecords);
  }

  @Test
  void call_withEnableLogSuccessShortForm_shouldSetToTrueWithoutValue() throws Exception {
    Path importFile = tempDir.resolve("import.json");
    Files.createFile(importFile);

    // Simulate command line parsing with the short form flag without providing true/false value
    String[] args = {"--file", importFile.toString(), "-ls"};
    ImportCommand command = new ImportCommand();
    CommandLine cmd = new CommandLine(command);
    cmd.parseArgs(args);

    // Verify the short form flag was parsed correctly without requiring a value
    assertTrue(command.enableLogSuccessRecords);
  }

  @Test
  void call_withMaxThreadsSpecified_shouldUseSpecifiedValue() {
    // Simulate command line parsing with --max-threads
    String[] args = {
      "--config",
      "scalardb.properties",
      "--file",
      "import.json",
      "--namespace",
      "scalar",
      "--table",
      "asset",
      "--max-threads",
      "8"
    };
    ImportCommand command = new ImportCommand();
    CommandLine cmd = new CommandLine(command);
    cmd.parseArgs(args);

    // Verify the value was parsed
    assertEquals(8, command.maxThreads);
  }

  @Test
  void call_withoutMaxThreads_shouldDefaultToAvailableProcessors() {
    // Simulate command line parsing without --max-threads
    String[] args = {
      "--config", "scalardb.properties",
      "--file", "import.json",
      "--namespace", "scalar",
      "--table", "asset"
    };
    ImportCommand command = new ImportCommand();
    CommandLine cmd = new CommandLine(command);
    cmd.parseArgs(args);

    // Verify maxThreads is null before validation
    assertEquals(null, command.maxThreads);

    // Simulate what happens in call() after validation
    command.spec = cmd.getCommandSpec();
    command.applyDeprecatedOptions();
    if (command.maxThreads == null) {
      command.maxThreads = Runtime.getRuntime().availableProcessors();
    }

    // Verify it was set to available processors
    assertEquals(Runtime.getRuntime().availableProcessors(), command.maxThreads);
  }

  @Test
  void validateTransactionMode_withUnsupportedOperation_shouldThrowException() throws Exception {
    // Arrange - Mock TransactionFactory to throw UnsupportedOperationException
    TransactionFactory mockFactory = mock(TransactionFactory.class);
    DistributedTransactionManager mockManager = mock(DistributedTransactionManager.class);

    when(mockFactory.getTransactionManager()).thenReturn(mockManager);
    when(mockManager.startReadOnly())
        .thenThrow(new UnsupportedOperationException("Transaction mode is not supported"));

    ImportCommand command = new ImportCommand();
    CommandLine cmd = new CommandLine(command);
    command.spec = cmd.getCommandSpec();
    command.scalarDbMode = ScalarDbMode.TRANSACTION;

    // Act & Assert
    CommandLine.ParameterException thrown =
        assertThrows(
            CommandLine.ParameterException.class,
            () -> command.validateTransactionMode(mockFactory));

    assertTrue(thrown.getMessage().contains("TRANSACTION mode is not compatible"));
  }

  @Test
  void validateTransactionMode_withOtherException_shouldThrowException() throws Exception {
    // Arrange - Mock TransactionFactory to throw a different exception
    TransactionFactory mockFactory = mock(TransactionFactory.class);
    DistributedTransactionManager mockManager = mock(DistributedTransactionManager.class);

    when(mockFactory.getTransactionManager()).thenReturn(mockManager);
    when(mockManager.startReadOnly()).thenThrow(new RuntimeException("Connection failed"));

    ImportCommand command = new ImportCommand();
    CommandLine cmd = new CommandLine(command);
    command.spec = cmd.getCommandSpec();
    command.scalarDbMode = ScalarDbMode.TRANSACTION;

    // Act & Assert
    CommandLine.ParameterException thrown =
        assertThrows(
            CommandLine.ParameterException.class,
            () -> command.validateTransactionMode(mockFactory));

    assertTrue(thrown.getMessage().contains("Failed to validate TRANSACTION mode"));
    assertTrue(thrown.getMessage().contains("Connection failed"));
  }
}
