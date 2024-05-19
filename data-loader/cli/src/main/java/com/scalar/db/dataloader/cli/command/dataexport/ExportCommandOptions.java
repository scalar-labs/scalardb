package com.scalar.db.dataloader.cli.command.dataexport;

import com.scalar.db.dataloader.core.ColumnKeyValue;
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

  @CommandLine.Option(
      names = {"--namespace", "-ns"},
      paramLabel = "<NAMESPACE>",
      required = true,
      description = "ScalarDB namespace containing the table to export data from")
  protected String namespace;

  @CommandLine.Option(
      names = {"--table", "-t"},
      paramLabel = "<TABLE_NAME>",
      required = true,
      description = "Name of the ScalarDB table to export data from")
  protected String tableName;

  @CommandLine.Option(
      names = {"--partition-key", "-pk"},
      paramLabel = "<PARTITION_KEY=VALUE>",
      description =
          "ScalarDB partition key and value in the format 'key=value'. If not provided, a full table scan is executed.")
  protected ColumnKeyValue partitionKeyValue;

  @CommandLine.Option(
      names = {"--scan-start-key", "-sk"},
      paramLabel = "<START_KEY=VALUE>",
      description =
          "Clustering key and value to mark the start of the scan in format 'columnName=value'")
  protected ColumnKeyValue scanStartKeyValue;

  @CommandLine.Option(
      names = {"--scan-end-key", "-ek"},
      paramLabel = "<END_KEY=VALUE>",
      description =
          "Clustering key and value to mark the end of the scan in format 'columnName=value'")
  protected ColumnKeyValue scanEndKeyValue;
}
