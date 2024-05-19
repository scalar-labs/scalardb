package com.scalar.db.dataloader.cli.command.dataexport;

import picocli.CommandLine;

/** A class to represent the command options for the export command. */
public class ExportCommandOptions {

  protected static final String DEFAULT_CONFIG_FILE_NAME = "scalardb.properties";

  @CommandLine.Option(
      names = {"--output-file", "-o"},
      paramLabel = "<OUTPUT_FILE>",
      description =
          "Path and name of the output file for the exported data (default: <table_name>.<format>)")
  protected String outputFilePath;

  @CommandLine.Option(
      names = {"--config-file", "-c"},
      paramLabel = "<CONFIG_FILE>",
      description = "Path to the ScalarDB configuration file (default: scalardb.properties)",
      defaultValue = DEFAULT_CONFIG_FILE_NAME)
  protected String configFilePath;
}
