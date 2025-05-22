package com.scalar.db.dataloader.cli.command.dataexport;

import com.scalar.db.api.Scan;
import com.scalar.db.dataloader.core.ColumnKeyValue;
import com.scalar.db.dataloader.core.FileFormat;
import java.util.ArrayList;
import java.util.List;
import picocli.CommandLine;

public class ExportCommandOptions {

  protected static final String DEFAULT_CONFIG_FILE_NAME = "scalardb.properties";

  @CommandLine.Option(
      names = {"--config", "-c"},
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
      paramLabel = "<TABLE>",
      required = true,
      description = "Name of the ScalarDB table to export data from")
  protected String table;

  @CommandLine.Option(
      names = {"--output-file", "-o"},
      paramLabel = "<FILE_NAME>",
      description =
          "Name of the output file for the exported data (default: export.<namespace>.<table>.<timestamp>.<format>)")
  protected String outputFileName;

  @CommandLine.Option(
      names = {"--output-dir", "-d"},
      paramLabel = "<DIRECTORY>",
      description =
          "Directory where the exported file should be saved (default: current directory)")
  protected String outputDirectory;

  @CommandLine.Option(
      names = {"--partition-key", "-pk"},
      paramLabel = "<KEY=VALUE>",
      description = "ScalarDB partition key and value in the format 'key=value'",
      converter = MultiColumnKeyValueConverter.class)
  protected List<ColumnKeyValue> partitionKeyValue;

  @CommandLine.Option(
      names = {"--format", "-fmt"},
      paramLabel = "<FORMAT>",
      description = "Format of the exported data file (json, csv, jsonl) (default: json)",
      defaultValue = "json")
  protected FileFormat outputFormat;

  @CommandLine.Option(
      names = {"--include-metadata", "-m"},
      description = "Include transaction metadata in the exported data (default: false)",
      defaultValue = "false")
  protected boolean includeTransactionMetadata;

  @CommandLine.Option(
      names = {"--max-threads", "-mt"},
      paramLabel = "<MAX_THREADS>",
      description =
          "Maximum number of threads to use for parallel processing (default: number of available processors)")
  protected int maxThreads;

  @CommandLine.Option(
      names = {"--start-key", "-sk"},
      paramLabel = "<KEY=VALUE>",
      description = "Clustering key and value to mark the start of the scan (inclusive)",
      converter = SingleColumnKeyValueConverter.class)
  protected ColumnKeyValue scanStartKeyValue;

  @CommandLine.Option(
      names = {"--start-inclusive", "-si"},
      description = "Make the start key inclusive (default: true)",
      defaultValue = "true")
  // TODO: test that -si false, works
  protected boolean scanStartInclusive;

  @CommandLine.Option(
      names = {"--end-key", "-ek"},
      paramLabel = "<KEY=VALUE>",
      description = "Clustering key and value to mark the end of the scan (inclusive)",
      converter = SingleColumnKeyValueConverter.class)
  protected ColumnKeyValue scanEndKeyValue;

  @CommandLine.Option(
      names = {"--end-inclusive", "-ei"},
      description = "Make the end key inclusive (default: true)",
      defaultValue = "true")
  protected boolean scanEndInclusive;

  @CommandLine.Option(
      names = {"--sort-by", "-s"},
      paramLabel = "<SORT_ORDER>",
      description = "Clustering key sorting order (asc, desc)",
      converter = ScanOrderingConverter.class)
  protected List<Scan.Ordering> sortOrders = new ArrayList<>();

  @CommandLine.Option(
      names = {"--projection", "-p"},
      paramLabel = "<COLUMN>",
      description = "Columns to include in the export (comma-separated)",
      split = ",")
  protected List<String> projectionColumns;

  @CommandLine.Option(
      names = {"--limit", "-l"},
      paramLabel = "<LIMIT>",
      description = "Maximum number of rows to export")
  protected int limit;

  @CommandLine.Option(
      names = {"--delimiter"},
      paramLabel = "<DELIMITER>",
      defaultValue = ",",
      description = "Delimiter character for CSV files (default: comma)")
  protected String delimiter;

  @CommandLine.Option(
      names = {"--no-header", "-nh"},
      description = "Exclude header row in CSV files (default: false)",
      defaultValue = "false")
  protected boolean excludeHeader;

  @CommandLine.Option(
      names = {"--pretty-print", "-pp"},
      description = "Pretty-print JSON output (default: false)",
      defaultValue = "false")
  protected boolean prettyPrintJson;

  @CommandLine.Option(
      names = {"--data-chunk-size", "-dcs"},
      description = "Size of the data chunk to process in a single task (default: 200)",
      defaultValue = "200")
  protected int dataChunkSize;
}
