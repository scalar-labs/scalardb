package com.scalar.db.dataloader.cli.command.dataimport;

import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.dataloader.core.ScalarDBMode;
import com.scalar.db.dataloader.core.dataimport.ImportMode;
import com.scalar.db.dataloader.core.dataimport.controlfile.ControlFileValidationLevel;
import picocli.CommandLine;

public class ImportCommandOptions {

  public static final String FILE_OPTION_NAME_LONG_FORMAT = "--file";

  @CommandLine.Option(
      names = {"--mode", "-m"},
      description = "ScalarDB mode (STORAGE, TRANSACTION) (default: STORAGE)",
      paramLabel = "<MODE>",
      defaultValue = "STORAGE")
  protected ScalarDBMode scalarDbMode;

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
          "Maximum number of threads to use for parallel processing (default: number of available processors)",
      defaultValue = "16")
  protected int maxThreads;

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

  @CommandLine.Option(
      names = {"--log-success", "-ls"},
      description = "Enable logging of successfully processed records (default: false)",
      defaultValue = "false")
  protected boolean logSuccessRecords;

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
      names = {"--header", "-h"},
      paramLabel = "<HEADER>",
      description =
          "Header row for the CSV/TSV import file (default: use the first line as the header)")
  protected String customHeaderRow;

  @CommandLine.Option(
      names = {"--data-chunk-size", "-dcs"},
      paramLabel = "<DATA_CHUNK_SIZE>",
      description = "Maximum number of records to be included in a single data chunk",
      defaultValue = "500")
  protected int dataChunkSize;

  @CommandLine.Option(
      names = {"--transaction-size", "-ts"},
      paramLabel = "<TRANSACTION_SIZE>",
      description =
          "Maximum number of put operations that are grouped together into one ScalarDB distributed transaction, only supported in ScalarDB transaction mode",
      defaultValue = "100")
  protected int transactionSize;

  @CommandLine.Option(
      names = {"--split-log-mode", "-slm"},
      paramLabel = "<SPLIT_LOG_MODE>",
      description = "Split log file into multiple files based on data chunks",
      defaultValue = "false")
  protected boolean splitLogMode;

  @CommandLine.Option(
      names = {"--data-chunk-queue-size", "-qs"},
      paramLabel = "<DATA_CHUNK_QUEUE_SIZE>",
      description = "Maximum number of data chunks that can be kept at a time for processing",
      defaultValue = "256")
  protected int dataChunkQueueSize;
}
