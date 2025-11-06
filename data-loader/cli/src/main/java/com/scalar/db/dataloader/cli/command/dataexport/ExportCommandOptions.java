package com.scalar.db.dataloader.cli.command.dataexport;

import com.scalar.db.api.Scan;
import com.scalar.db.dataloader.core.ColumnKeyValue;
import com.scalar.db.dataloader.core.FileFormat;
import java.util.ArrayList;
import java.util.List;
import picocli.CommandLine;

public class ExportCommandOptions {

  protected static final String DEFAULT_CONFIG_FILE_NAME = "scalardb.properties";
  public static final String START_INCLUSIVE_OPTION = "--start-inclusive";
  public static final String START_INCLUSIVE_OPTION_SHORT = "-si";
  public static final String DEPRECATED_START_EXCLUSIVE_OPTION = "--start-exclusive";
  public static final String END_INCLUSIVE_OPTION = "--end-inclusive";
  public static final String END_INCLUSIVE_OPTION_SHORT = "-ei";
  public static final String DEPRECATED_END_EXCLUSIVE_OPTION = "--end-exclusive";
  public static final String THREADS_OPTION = "--threads";
  public static final String DEPRECATED_MAX_THREADS_OPTION = "--max-threads";

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
      names = {"--threads"},
      paramLabel = "<THREADS>",
      description =
          "Number of threads to use for parallel processing (default: number of available processors)")
  protected int threadCount;

  // Deprecated option - kept for backward compatibility
  @CommandLine.Option(
      names = {DEPRECATED_MAX_THREADS_OPTION, "-mt"},
      paramLabel = "<MAX_THREADS>",
      description = "Deprecated: Use --threads instead",
      hidden = true)
  @Deprecated
  protected Integer maxThreadsDeprecated;

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

  // Deprecated option - kept for backward compatibility
  @CommandLine.Option(
      names = {DEPRECATED_START_EXCLUSIVE_OPTION},
      description = "Deprecated: Use --start-inclusive instead (inverted logic)",
      hidden = true)
  @Deprecated
  protected Boolean startExclusiveDeprecated;

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

  // Deprecated option - kept for backward compatibility
  @CommandLine.Option(
      names = {DEPRECATED_END_EXCLUSIVE_OPTION},
      description = "Deprecated: Use --end-inclusive instead (inverted logic)",
      hidden = true)
  @Deprecated
  protected Boolean endExclusiveDeprecated;

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

  /**
   * Applies deprecated option values if they are set.
   *
   * <p>This method is called AFTER validateDeprecatedOptions(), so we are guaranteed that both the
   * deprecated and new options were not specified together. If we reach this point, only the
   * deprecated option was provided by the user.
   */
  public void applyDeprecatedOptions() {
    // If the deprecated option is set, use its value (inverted logic)
    if (startExclusiveDeprecated != null) {
      scanStartInclusive = !startExclusiveDeprecated;
    }

    // If the deprecated option is set, use its value (inverted logic)
    if (endExclusiveDeprecated != null) {
      scanEndInclusive = !endExclusiveDeprecated;
    }

    // If the deprecated option is set, use its value
    if (maxThreadsDeprecated != null) {
      threadCount = maxThreadsDeprecated;
    }
  }
}
