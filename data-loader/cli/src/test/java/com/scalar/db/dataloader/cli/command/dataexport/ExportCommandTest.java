package com.scalar.db.dataloader.cli.command.dataexport;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.scalar.db.dataloader.core.DataLoaderError;
import com.scalar.db.dataloader.core.FileFormat;
import java.io.File;
import java.nio.file.Paths;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class ExportCommandTest {

  private ExportCommand exportCommand;

  @BeforeEach
  void setUp() {
    exportCommand = new ExportCommand();
    CommandLine cmd = new CommandLine(exportCommand);
    // Parse empty args to properly initialize spec.commandLine() reference
    try {
      cmd.parseArgs();
    } catch (Exception e) {
      // Ignore parse errors for missing required options - we'll set fields directly in tests
    }
    exportCommand.spec = cmd.getCommandSpec();
  }

  @AfterEach
  void removeFileIfCreated() {
    // To remove generated file if it is present
    String filePath = Paths.get("").toAbsolutePath() + "/sample.json";
    File file = new File(filePath);
    if (file.exists()) {
      file.deleteOnExit();
    }
  }

  @Test
  void call_withBlankScalarDBConfigurationFile_shouldThrowException() {
    exportCommand.configFilePath = "";
    exportCommand.dataChunkSize = 100;
    exportCommand.maxThreads = 4;
    exportCommand.namespace = "scalar";
    exportCommand.table = "asset";
    exportCommand.outputDirectory = "";
    exportCommand.outputFileName = "sample.json";
    exportCommand.outputFormat = FileFormat.JSON;
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            exportCommand::call,
            "Expected to throw FileNotFound exception as configuration path is invalid");
    Assertions.assertEquals(
        DataLoaderError.CONFIG_FILE_PATH_BLANK.buildMessage(), thrown.getMessage());
  }

  @Test
  void call_withInvalidScalarDBConfigurationFile_shouldReturnOne() throws Exception {
    exportCommand.configFilePath = "scalardb.properties";
    exportCommand.dataChunkSize = 100;
    exportCommand.maxThreads = 4;
    exportCommand.namespace = "scalar";
    exportCommand.table = "asset";
    exportCommand.outputDirectory = "";
    exportCommand.outputFileName = "sample.json";
    exportCommand.outputFormat = FileFormat.JSON;
    Assertions.assertEquals(1, exportCommand.call());
  }

  @Test
  void call_withBothStartExclusiveAndStartInclusive_shouldThrowException() {
    // Simulate command line parsing with both deprecated and new options
    String[] args = {
      "--config",
      "scalardb.properties",
      "--namespace",
      "scalar",
      "--table",
      "asset",
      "--format",
      "JSON",
      "--start-exclusive=false",
      "--start-inclusive=false"
    };
    ExportCommand command = new ExportCommand();
    CommandLine cmd = new CommandLine(command);
    // Parse args - this will trigger our validation
    cmd.parseArgs(args);

    // Now call the command, which should throw the validation error
    CommandLine.ParameterException thrown =
        assertThrows(
            CommandLine.ParameterException.class,
            command::call,
            "Expected to throw ParameterException when both deprecated and new options are specified");
    Assertions.assertTrue(
        thrown
            .getMessage()
            .contains(
                "Cannot specify both deprecated option '--start-exclusive' and new option '--start-inclusive'"));
  }

  @Test
  void call_withBothEndExclusiveAndEndInclusive_shouldThrowException() {
    // Simulate command line parsing with both deprecated and new options
    String[] args = {
      "--config",
      "scalardb.properties",
      "--namespace",
      "scalar",
      "--table",
      "asset",
      "--format",
      "JSON",
      "--end-exclusive=false",
      "--end-inclusive=false"
    };
    ExportCommand command = new ExportCommand();
    CommandLine cmd = new CommandLine(command);
    // Parse args - this will trigger our validation
    cmd.parseArgs(args);

    // Now call the command, which should throw the validation error
    CommandLine.ParameterException thrown =
        assertThrows(
            CommandLine.ParameterException.class,
            command::call,
            "Expected to throw ParameterException when both deprecated and new options are specified");
    Assertions.assertTrue(
        thrown
            .getMessage()
            .contains(
                "Cannot specify both deprecated option '--end-exclusive' and new option '--end-inclusive'"));
  }
}
