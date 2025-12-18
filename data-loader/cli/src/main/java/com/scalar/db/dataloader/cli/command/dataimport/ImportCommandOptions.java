package com.scalar.db.dataloader.cli.command.dataimport;

import com.scalar.db.dataloader.cli.ScalarDbMode;
import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.dataloader.core.dataimport.ImportMode;
import com.scalar.db.dataloader.core.dataimport.controlfile.ControlFileValidationLevel;
import picocli.CommandLine;

@SuppressWarnings("deprecation")
public class ImportCommandOptions {

  public static final String FILE_OPTION_NAME_LONG_FORMAT = "--file";
  public static final String MAX_THREADS_OPTION = "--max-threads";
  public static final String MAX_THREADS_OPTION_SHORT = "-mt";
  public static final String DEPRECATED_THREADS_OPTION = "--threads";

  public static final String ENABLE_LOG_SUCCESS_RECORDS_OPTION = "--enable-log-success";
  public static final String ENABLE_LOG_SUCCESS_RECORDS_OPTION_SHORT = "-ls";
  public static final String DEPRECATED_LOG_SUCCESS_RECORDS_OPTION = "--log-success";

  @CommandLine.Option(
      names = {"--mode", "-m"},
      description = "ScalarDB mode (STORAGE, TRANSACTION) (default: STORAGE)",
      paramLabel = "<MODE>",
      defaultValue = "STORAGE")
  protected ScalarDbMode scalarDbMode;

  @CommandLine.Option(
      names = {"--config", "-c"},
      paramLabel = "<CONFIG_FILE>",
      description = "Path to the ScalarDB configuration file (default: scalardb.properties)",
      defaultValue = "scalardb.properties")
  protected String configFilePath;

  @CommandLine.Option(
      names = {FILE_OPTION_NAME_LONG_FORMAT, "-f"},
      paramLabel = "<SOURCE_FILE>",
      description = "Path to the import source file",
      required = true)
  protected String sourceFilePath;

  @CommandLine.Option(
      names = {"--max-threads", "-mt"},
      paramLabel = "<MAX_THREADS>",
      description =
          "Maximum number of threads to use for parallel processing (default: number of available processors)")
  protected Integer maxThreads;

  /**
   * @deprecated As of release 3.17.0. Will be removed in release 4.0.0. Use --max-threads instead
   */
  @Deprecated
  @CommandLine.Option(
      names = {DEPRECATED_THREADS_OPTION},
      paramLabel = "<THREADS>",
      description = "Deprecated: Use --max-threads instead",
      hidden = true)
  protected Integer threadsDeprecated;

  @CommandLine.Option(
      names = {"--namespace", "-ns"},
      paramLabel = "<NAMESPACE>",
      description = "ScalarDB namespace containing the table to import data into")
  protected String namespace;

  @CommandLine.Option(
      names = {"--table", "-t"},
      paramLabel = "<TABLE_NAME>",
      description = "Name of the ScalarDB table to import data into")
  protected String tableName;

  @CommandLine.Option(
      names = {"--control-file", "-cf"},
      paramLabel = "<CONTROL_FILE>",
      description = "Path to the JSON control file for data mapping")
  protected String controlFilePath;

  /**
   * @deprecated As of release 3.17.0. Will be removed in release 4.0.0. Use --enable-log-success
   *     instead
   */
  @Deprecated
  @CommandLine.Option(
      names = {"--log-success"},
      description = "Deprecated: Use --enable-log-success",
      hidden = true)
  protected boolean logSuccessRecordsDeprecated;

  @CommandLine.Option(
      names = {"--enable-log-success", "-ls"},
      description = "Enable logging of successfully processed records (default: false)",
      defaultValue = "false")
  protected boolean enableLogSuccessRecords;

  @CommandLine.Option(
      names = {"--log-dir", "-ld"},
      paramLabel = "<LOG_DIR>",
      description = "Directory where log files should be stored (default: logs)",
      defaultValue = "logs")
  protected String logDirectory;

  @CommandLine.Option(
      names = {"--format", "-fmt"},
      paramLabel = "<FORMAT>",
      description = "Format of the import source file (JSON, CSV, JSONL) (default: JSON)",
      defaultValue = "JSON")
  protected FileFormat sourceFileFormat;

  @CommandLine.Option(
      names = {"--require-all-columns", "-rac"},
      description = "Require all columns to be present in the source file (default: false)",
      defaultValue = "false")
  protected boolean requireAllColumns;

  @CommandLine.Option(
      names = {"--pretty-print", "-pp"},
      description = "Enable pretty printing for JSON output (default: false)",
      defaultValue = "false")
  protected boolean prettyPrint;

  @CommandLine.Option(
      names = {"--ignore-nulls", "-in"},
      description = "Ignore null values in the source file during import (default: false)",
      defaultValue = "false")
  protected boolean ignoreNullValues;

  @CommandLine.Option(
      names = {"--log-raw-record", "-lr"},
      description = "Include the original source record in the log file output (default: false)",
      defaultValue = "false")
  protected boolean logRawRecord;

  @CommandLine.Option(
      names = {"--control-file-validation", "-cfv"},
      paramLabel = "<VALIDATION_LEVEL>",
      description =
          "Level of validation to perform on control file data mappings (FULL, KEYS, MAPPED) (default: MAPPED)",
      defaultValue = "MAPPED")
  protected ControlFileValidationLevel controlFileValidation;

  @CommandLine.Option(
      names = {"--import-mode", "-im"},
      paramLabel = "<IMPORT_MODE>",
      description = "Import mode (INSERT, UPDATE, UPSERT) (default: INSERT)",
      defaultValue = "INSERT")
  protected ImportMode importMode;

  @CommandLine.Option(
      names = {"--delimiter", "-d"},
      paramLabel = "<DELIMITER>",
      description = "Delimiter character used in the CSV import file (default: comma for CSV)",
      defaultValue = ",")
  protected char delimiter;

  @CommandLine.Option(
      names = {"--header", "-hdr"},
      paramLabel = "<HEADER>",
      description =
          "Header row for the CSV/TSV import file (default: use the first line as the header)")
  protected String customHeaderRow;

  /**
   * @deprecated As of release 3.17.0. Will be removed in release 4.0.0. Data chunking is no longer
   *     used; the import process now works with transactions only.
   */
  @Deprecated
  @CommandLine.Option(
      names = {"--data-chunk-size", "-dcs"},
      paramLabel = "<DATA_CHUNK_SIZE>",
      description = "Deprecated: This option is no longer used and will be removed in 4.0.0",
      hidden = true)
  protected Integer dataChunkSizeDeprecated;

  @CommandLine.Option(
      names = {"--transaction-size", "-ts"},
      paramLabel = "<TRANSACTION_SIZE>",
      description =
          "Maximum number of put operations that are grouped together into one ScalarDB distributed transaction, only supported in ScalarDB transaction mode (default: 100)",
      defaultValue = "100")
  protected int transactionSize;

  /**
   * @deprecated As of release 3.17.0. Will be removed in release 4.0.0. Data chunking is no longer
   *     used; the import process now works with transactions only.
   */
  @Deprecated
  @CommandLine.Option(
      names = {"--split-log-mode", "-slm"},
      paramLabel = "<SPLIT_LOG_MODE>",
      description = "Deprecated: This option is no longer used and will be removed in 4.0.0",
      hidden = true)
  protected boolean splitLogModeDeprecated;

  /**
   * @deprecated As of release 3.17.0. Will be removed in release 4.0.0. Data chunking is no longer
   *     used; the import process now works with transactions only.
   */
  @Deprecated
  @CommandLine.Option(
      names = {"--data-chunk-queue-size", "-qs"},
      paramLabel = "<DATA_CHUNK_QUEUE_SIZE>",
      description = "Deprecated: This option is no longer used and will be removed in 4.0.0",
      hidden = true)
  protected Integer dataChunkQueueSizeDeprecated;

  /**
   * Applies deprecated option values if they are set.
   *
   * <p>This method is called AFTER validateDeprecatedOptions(), so we are guaranteed that both the
   * deprecated and new options were not specified together. If we reach this point, only the
   * deprecated option was provided by the user.
   */
  public void applyDeprecatedOptions() {
    // If the deprecated option is set, use its value
    if (threadsDeprecated != null) {
      maxThreads = threadsDeprecated;
    }
    if (logSuccessRecordsDeprecated) {
      enableLogSuccessRecords = true;
    }
  }
}
