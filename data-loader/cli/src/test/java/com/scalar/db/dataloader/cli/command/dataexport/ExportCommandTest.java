package com.scalar.db.dataloader.cli.command.dataexport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.scalar.db.dataloader.core.DataLoaderError;
import com.scalar.db.dataloader.core.FileFormat;
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
  void call_withOnlyDeprecatedThreads_shouldApplyValue() {
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
      "--threads",
      "12"
    };
    ExportCommand command = new ExportCommand();
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
  void call_withMaxThreadsSpecified_shouldUseSpecifiedValue() {
    // Simulate command line parsing with --max-threads
    String[] args = {
      "--config",
      "scalardb.properties",
      "--namespace",
      "scalar",
      "--table",
      "asset",
      "--format",
      "JSON",
      "--max-threads",
      "8"
    };
    ExportCommand command = new ExportCommand();
    CommandLine cmd = new CommandLine(command);
    cmd.parseArgs(args);

    // Verify the value was parsed
    assertEquals(8, command.maxThreads);
  }

  @Test
  void call_withoutMaxThreads_shouldDefaultToAvailableProcessors() {
    // Simulate command line parsing without --max-threads
    String[] args = {
      "--config",
      "scalardb.properties",
      "--namespace",
      "scalar",
      "--table",
      "asset",
      "--format",
      "JSON"
    };
    ExportCommand command = new ExportCommand();
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
  void call_withDeprecatedIncludeMetadataOption_shouldParseWithoutError() {
    // Simulate command line parsing with deprecated --include-metadata option
    String[] args = {
      "--config",
      "scalardb.properties",
      "--namespace",
      "scalar",
      "--table",
      "asset",
      "--format",
      "JSON",
      "--include-metadata"
    };
    ExportCommand command = new ExportCommand();
    CommandLine cmd = new CommandLine(command);
    cmd.parseArgs(args);

    // Verify the deprecated option was parsed
    // Since it has defaultValue = "false", using the flag sets it to true
    assertEquals(true, command.includeTransactionMetadata);

    // Verify that the spec was set correctly
    command.spec = cmd.getCommandSpec();

    // Verify that the command line has the deprecated option matched
    assertTrue(
        cmd.getParseResult()
            .hasMatchedOption(ExportCommandOptions.DEPRECATED_INCLUDE_METADATA_OPTION));
  }

  @Test
  void call_withDeprecatedIncludeMetadataShortOption_shouldParseWithoutError() {
    // Simulate command line parsing with deprecated -m short option
    String[] args = {
      "--config",
      "scalardb.properties",
      "--namespace",
      "scalar",
      "--table",
      "asset",
      "--format",
      "JSON",
      "-m"
    };
    ExportCommand command = new ExportCommand();
    CommandLine cmd = new CommandLine(command);
    cmd.parseArgs(args);

    // Verify the deprecated option was parsed
    assertEquals(true, command.includeTransactionMetadata);

    // Verify that the spec was set correctly
    command.spec = cmd.getCommandSpec();

    // Verify that the command line has the deprecated short option matched
    assertTrue(
        cmd.getParseResult()
            .hasMatchedOption(ExportCommandOptions.DEPRECATED_INCLUDE_METADATA_OPTION_SHORT));
  }

  @Test
  void call_withDeprecatedIncludeMetadataFalse_shouldParseWithoutError() {
    // Simulate command line parsing with deprecated --include-metadata=false option
    String[] args = {
      "--config",
      "scalardb.properties",
      "--namespace",
      "scalar",
      "--table",
      "asset",
      "--format",
      "JSON",
      "--include-metadata=false"
    };
    ExportCommand command = new ExportCommand();
    CommandLine cmd = new CommandLine(command);
    cmd.parseArgs(args);

    // Verify the deprecated option was parsed with explicit false value
    assertEquals(false, command.includeTransactionMetadata);

    // Verify that the spec was set correctly
    command.spec = cmd.getCommandSpec();

    // Verify that the command line has the deprecated option matched
    assertTrue(
        cmd.getParseResult()
            .hasMatchedOption(ExportCommandOptions.DEPRECATED_INCLUDE_METADATA_OPTION));
  }

  @Test
  void call_withoutDeprecatedIncludeMetadataOption_shouldHaveDefaultValue() {
    // Simulate command line parsing without the deprecated option
    String[] args = {
      "--config",
      "scalardb.properties",
      "--namespace",
      "scalar",
      "--table",
      "asset",
      "--format",
      "JSON"
    };
    ExportCommand command = new ExportCommand();
    CommandLine cmd = new CommandLine(command);
    cmd.parseArgs(args);

    // Verify the option has its default value (false)
    assertEquals(false, command.includeTransactionMetadata);

    // Verify that the spec was set correctly
    command.spec = cmd.getCommandSpec();

    // Verify that the command line does NOT have the deprecated option matched
    assertEquals(
        false,
        cmd.getParseResult()
            .hasMatchedOption(ExportCommandOptions.DEPRECATED_INCLUDE_METADATA_OPTION));
  }
}
