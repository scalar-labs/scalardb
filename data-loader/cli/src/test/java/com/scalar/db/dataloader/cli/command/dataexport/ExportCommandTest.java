package com.scalar.db.dataloader.cli.command.dataexport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.scalar.db.dataloader.core.DataLoaderError;
import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.dataloader.core.ScalarDbMode;
import java.io.File;
import java.nio.file.Paths;
import org.junit.jupiter.api.AfterEach;
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
    assertEquals(DataLoaderError.CONFIG_FILE_PATH_BLANK.buildMessage(), thrown.getMessage());
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
    assertEquals(1, exportCommand.call());
  }

  @Test
  void call_withBothStartExclusiveAndStartInclusive_shouldThrowException() {
    assertBothDeprecatedAndNewOptionsThrowException(
        "--start-exclusive=false",
        "--start-inclusive=false",
        "--start-exclusive",
        "--start-inclusive");
  }

  @Test
  void call_withBothEndExclusiveAndEndInclusive_shouldThrowException() {
    assertBothDeprecatedAndNewOptionsThrowException(
        "--end-exclusive=false", "--end-inclusive=false", "--end-exclusive", "--end-inclusive");
  }

  /**
   * Helper method to test that using both deprecated and new options together throws an exception.
   *
   * @param deprecatedOptionArg the deprecated option with value (e.g., "--start-exclusive=false")
   * @param newOptionArg the new option with value (e.g., "--start-inclusive=false")
   * @param deprecatedOptionName the deprecated option name for error message verification
   * @param newOptionName the new option name for error message verification
   */
  private void assertBothDeprecatedAndNewOptionsThrowException(
      String deprecatedOptionArg,
      String newOptionArg,
      String deprecatedOptionName,
      String newOptionName) {
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
      deprecatedOptionArg,
      newOptionArg
    };
    ExportCommand command = new ExportCommand();
    CommandLine cmd = new CommandLine(command);
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
                "Cannot specify both deprecated option '"
                    + deprecatedOptionName
                    + "' and new option '"
                    + newOptionName
                    + "'"));
  }

  @Test
  void call_withOnlyDeprecatedStartExclusive_shouldApplyInvertedValue() {
    // Simulate command line parsing with only deprecated option
    String[] args = {
      "--config",
      "scalardb.properties",
      "--namespace",
      "scalar",
      "--table",
      "asset",
      "--format",
      "JSON",
      "--start-exclusive=true"
    };
    ExportCommand command = new ExportCommand();
    CommandLine cmd = new CommandLine(command);
    cmd.parseArgs(args);

    // Verify the deprecated value was parsed
    assertEquals(true, command.startExclusiveDeprecated);

    // Apply deprecated options (this is what the command does after validation)
    command.applyDeprecatedOptions();

    // Verify the value was applied with inverted logic
    // start-exclusive=true should become start-inclusive=false
    assertEquals(false, command.scanStartInclusive);
  }

  @Test
  void call_withOnlyDeprecatedEndExclusive_shouldApplyInvertedValue() {
    // Simulate command line parsing with only deprecated option
    String[] args = {
      "--config",
      "scalardb.properties",
      "--namespace",
      "scalar",
      "--table",
      "asset",
      "--format",
      "JSON",
      "--end-exclusive=false"
    };
    ExportCommand command = new ExportCommand();
    CommandLine cmd = new CommandLine(command);
    cmd.parseArgs(args);

    // Verify the deprecated value was parsed
    assertEquals(false, command.endExclusiveDeprecated);

    // Apply deprecated options (this is what the command does after validation)
    command.applyDeprecatedOptions();

    // Verify the value was applied with inverted logic
    // end-exclusive=false should become end-inclusive=true
    assertEquals(true, command.scanEndInclusive);
  }

  @Test
  void call_withScalarDBModeTransaction_WithInvalidConfigurationFile_shouldReturnOne()
      throws Exception {
    ExportCommand exportCommand = new ExportCommand();
    exportCommand.configFilePath = "scalardb.properties";
    exportCommand.dataChunkSize = 100;
    exportCommand.namespace = "scalar";
    exportCommand.table = "asset";
    exportCommand.outputDirectory = "";
    exportCommand.outputFileName = "sample.json";
    exportCommand.outputFormat = FileFormat.JSON;
    exportCommand.scalarDbMode = ScalarDbMode.TRANSACTION;
    Assertions.assertEquals(1, exportCommand.call());
  }
}
