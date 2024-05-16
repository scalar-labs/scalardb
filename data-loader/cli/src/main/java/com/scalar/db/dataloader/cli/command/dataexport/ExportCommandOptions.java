package com.scalar.db.dataloader.cli.command.dataexport;

import com.scalar.db.api.Scan;
import com.scalar.db.dataloader.cli.command.ColumnKeyValue;
import com.scalar.db.dataloader.core.FileFormat;
import java.util.ArrayList;
import java.util.List;
import picocli.CommandLine;

/** A class to represent the command options for the export command. */
public class ExportCommandOptions {

  protected static final String DEFAULT_CONFIG_FILE_NAME = "scalardb.properties";

  @CommandLine.Option(
      names = {"--config-file", "-c"},
      paramLabel = "<CONFIG_FILE>",
      description =
          "Path to the ScalarDB configuration file (default: " + DEFAULT_CONFIG_FILE_NAME + ")",
      defaultValue = DEFAULT_CONFIG_FILE_NAME)
  protected String configFilePath;

  @CommandLine.Option(
      names = {"--namespace", "-ns"},
      paramLabel = "<NAMESPACE>",
      required = true,
      description = "ScalarDB namespace containing the table to export data from")
  protected String namespace;

  @CommandLine.Option(
      names = {"--table-name", "-t"},
      paramLabel = "<TABLE_NAME>",
      required = true,
      description = "Name of the ScalarDB table to export data from")
  protected String tableName;

  @CommandLine.Option(
      names = {"--output-file", "-o"},
      paramLabel = "<OUTPUT_FILE>",
      description =
          "Path and name of the output file for the exported data (default: <table_name>.<format>)")
  protected String outputFilePath;

  @CommandLine.Option(
      names = {"--partition-key", "-pk"},
      paramLabel = "<PARTITION_KEY=VALUE>",
      description =
          "ScalarDB partition key and value in the format 'key=value'. If not provided, a full table scan is executed.")
  protected ColumnKeyValue partitionKeyValue;

  @CommandLine.Option(
      names = {"--output-format", "-f"},
      paramLabel = "<OUTPUT_FORMAT>",
      description = "Format of the exported data file (JSON, CSV) (default: JSON)",
      defaultValue = "JSON")
  protected FileFormat outputFormat;

  @CommandLine.Option(
      names = {"--exclude-metadata", "-em"},
      description = "Exclude transaction metadata columns from the exported data (default: false)",
      defaultValue = "false")
  protected boolean excludeTransactionMetadata;

  @CommandLine.Option(
      names = {"--max-threads", "-mt"},
      paramLabel = "<MAX_THREADS>",
      description =
          "Maximum number of threads to use for parallel processing (default: number of available processors)")
  protected int maxThreads;

  @CommandLine.Option(
      names = {"--scan-start-key", "-sk"},
      paramLabel = "<START_KEY=VALUE>",
      description = "Clustering key and value to mark the start of the scan in format 'key=value'")
  protected ColumnKeyValue scanStartKeyValue;

  @CommandLine.Option(
      names = {"--scan-start-exclusive", "-se"},
      description = "Make the start key exclusive (default: inclusive)",
      defaultValue = "false")
  protected boolean scanStartExclusive;

  @CommandLine.Option(
      names = {"--scan-end-key", "-ek"},
      paramLabel = "<END_KEY=VALUE>",
      description = "Clustering key and value to mark the end of the scan in format 'key=value'")
  protected ColumnKeyValue scanEndKeyValue;

  @CommandLine.Option(
      names = {"--scan-end-exclusive", "-ee"},
      description = "Make the end key exclusive (default: inclusive)",
      defaultValue = "false")
  protected boolean scanEndExclusive;

  @CommandLine.Option(
      names = {"--sort-order", "-so"},
      paramLabel = "<SORT_ORDER>",
      description =
          "List of clustering keys or secondary index names to sort the exported data by, in the format 'key1=asc,key2=desc' (comma-separated, asc or desc for each key)")
  protected List<Scan.Ordering> sortOrders = new ArrayList<>();

  @CommandLine.Option(
      names = {"--projection-columns", "-pc"},
      paramLabel = "<COLUMN1,COLUMN2,...>",
      description = "Comma-separated list of columns to include in the export",
      split = ",")
  protected List<String> projectionColumns;

  @CommandLine.Option(
      names = {"--limit", "-l"},
      paramLabel = "<LIMIT>",
      description = "Maximum number of rows to export")
  protected int limit;

  @CommandLine.Option(
      names = {"--csv-delimiter", "-d"},
      paramLabel = "<DELIMITER>",
      description = "Delimiter character for CSV files (default: semi-colon)")
  protected String csvDelimiter;

  @CommandLine.Option(
      names = {"--exclude-header", "-eh"},
      description = "Exclude header row in CSV files (default: false)",
      defaultValue = "false")
  protected boolean excludeHeader;

  @CommandLine.Option(
      names = {"--pretty-print", "-pp"},
      description = "Pretty-print the exported JSON data (default: false)",
      defaultValue = "false")
  protected boolean prettyPrint;

  @CommandLine.Option(
      names = {"--chunk-size", "-cs"},
      paramLabel = "<CHUNK_SIZE>",
      description = "Number of rows to process per chunk during export (default: 1000)",
      defaultValue = "1000")
  protected int chunkSize;
}
