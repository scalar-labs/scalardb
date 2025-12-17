package com.scalar.db.dataloader.core;

import com.scalar.db.common.error.Category;
import com.scalar.db.common.error.ScalarDbError;

public enum DataLoaderError implements ScalarDbError {

  //
  // Errors for the user error category
  //
  INVALID_DATA_CHUNK_QUEUE_SIZE(
      Category.USER_ERROR, "0000", "Data chunk queue size must be greater than 0", "", ""),
  DIRECTORY_WRITE_ACCESS_NOT_ALLOWED(
      Category.USER_ERROR,
      "0001",
      "The directory '%s' does not have write permissions. Please ensure that the current user has write access to the directory",
      "",
      ""),
  DIRECTORY_CREATE_FAILED(
      Category.USER_ERROR,
      "0002",
      "Failed to create the directory '%s'. Please check if you have sufficient permissions and if there are any file system restrictions. Details: %s",
      "",
      ""),
  MISSING_DIRECTORY_NOT_ALLOWED(
      Category.USER_ERROR, "0003", "Directory path cannot be null or empty", "", ""),
  MISSING_FILE_EXTENSION(
      Category.USER_ERROR,
      "0004",
      "No file extension was found in the provided file name %s",
      "",
      ""),
  INVALID_FILE_EXTENSION(
      Category.USER_ERROR,
      "0005",
      "Invalid file extension: %s. Allowed extensions are: %s",
      "",
      ""),
  INVALID_COLUMN_NON_EXISTENT(
      Category.USER_ERROR,
      "0006",
      "Invalid key: Column %s does not exist in the table %s in namespace %s",
      "",
      ""),
  INVALID_BASE64_ENCODING_FOR_COLUMN_VALUE(
      Category.USER_ERROR,
      "0007",
      "Invalid base64 encoding for blob value '%s' for column %s in table %s in namespace %s",
      "",
      ""),
  INVALID_NUMBER_FORMAT_FOR_COLUMN_VALUE(
      Category.USER_ERROR,
      "0008",
      "Invalid number '%s' specified for column %s in table %s in namespace %s",
      "",
      ""),
  ERROR_METHOD_NULL_ARGUMENT(
      Category.USER_ERROR, "0009", "Method null argument not allowed", "", ""),
  CLUSTERING_KEY_NOT_FOUND(
      Category.USER_ERROR, "0010", "The provided clustering key %s was not found", "", ""),
  INVALID_PROJECTION(Category.USER_ERROR, "0011", "The column '%s' was not found", "", ""),
  INCOMPLETE_PARTITION_KEY(
      Category.USER_ERROR,
      "0012",
      "The provided partition key is incomplete. Required key: %s",
      "",
      ""),
  CLUSTERING_KEY_ORDER_MISMATCH(
      Category.USER_ERROR,
      "0013",
      "The provided clustering-key order does not match the table schema. Required order: %s",
      "",
      ""),
  PARTITION_KEY_ORDER_MISMATCH(
      Category.USER_ERROR,
      "0014",
      "The provided partition-key order does not match the table schema. Required order: %s",
      "",
      ""),
  MISSING_NAMESPACE_OR_TABLE(
      Category.USER_ERROR, "0015", "Missing namespace or table: %s, %s", "", ""),
  TABLE_METADATA_RETRIEVAL_FAILED(
      Category.USER_ERROR, "0016", "Failed to retrieve table metadata. Details: %s", "", ""),
  DUPLICATE_DATA_MAPPINGS(
      Category.USER_ERROR,
      "0017",
      "Duplicate data mappings found for table '%s' in the control file",
      "",
      ""),
  MISSING_COLUMN_MAPPING(
      Category.USER_ERROR,
      "0018",
      "No mapping found for column '%s' in table '%s' in the control file. Control file validation set at 'FULL'. All columns need to be mapped",
      "",
      ""),
  CONTROL_FILE_MISSING_DATA_MAPPINGS(
      Category.USER_ERROR, "0019", "The control file is missing data mappings", "", ""),
  TARGET_COLUMN_NOT_FOUND(
      Category.USER_ERROR,
      "0020",
      "The target column '%s' for source field '%s' could not be found in table '%s'",
      "",
      ""),
  MISSING_PARTITION_KEY(
      Category.USER_ERROR,
      "0021",
      "The required partition key '%s' is missing in the control file mapping for table '%s'",
      "",
      ""),
  MISSING_CLUSTERING_KEY(
      Category.USER_ERROR,
      "0022",
      "The required clustering key '%s' is missing in the control file mapping for table '%s'",
      "",
      ""),
  MULTIPLE_MAPPINGS_FOR_COLUMN_FOUND(
      Category.USER_ERROR,
      "0023",
      "Duplicated data mappings found for column '%s' in table '%s'",
      "",
      ""),
  MISSING_CLUSTERING_KEY_COLUMN(
      Category.USER_ERROR,
      "0024",
      "Missing required field or column mapping for clustering key %s",
      "",
      ""),
  MISSING_PARTITION_KEY_COLUMN(
      Category.USER_ERROR,
      "0025",
      "Missing required field or column mapping for partition key %s",
      "",
      ""),
  MISSING_COLUMN(Category.USER_ERROR, "0026", "Missing field or column mapping for %s", "", ""),
  VALUE_TO_STRING_CONVERSION_FAILED(
      Category.USER_ERROR,
      "0027",
      "Something went wrong while converting the ScalarDB values to strings. The table metadata and Value datatype probably do not match. Details: %s",
      "",
      ""),
  FILE_FORMAT_NOT_SUPPORTED(
      Category.USER_ERROR, "0028", "The provided file format is not supported: %s", "", ""),
  COULD_NOT_FIND_PARTITION_KEY(
      Category.USER_ERROR, "0029", "Could not find the partition key", "", ""),
  UPSERT_INSERT_MISSING_COLUMNS(
      Category.USER_ERROR,
      "0030",
      "The source record needs to contain all fields if the UPSERT turns into an INSERT",
      "",
      ""),
  DATA_ALREADY_EXISTS(Category.USER_ERROR, "0031", "Record already exists", "", ""),
  DATA_NOT_FOUND(Category.USER_ERROR, "0032", "Record was not found", "", ""),
  COULD_NOT_FIND_CLUSTERING_KEY(
      Category.USER_ERROR, "0033", "Could not find the clustering key", "", ""),
  TABLE_METADATA_MISSING(Category.USER_ERROR, "0034", "No table metadata found", "", ""),
  MISSING_SOURCE_FIELD(
      Category.USER_ERROR,
      "0035",
      "The data mapping source field '%s' for table '%s' is missing in the JSON data record",
      "",
      ""),
  CSV_DATA_MISMATCH(
      Category.USER_ERROR, "0036", "The CSV row: %s does not match header: %s", "", ""),
  JSON_CONTENT_START_ERROR(
      Category.USER_ERROR, "0037", "Expected JSON file content to be an array", "", ""),
  IMPORT_TARGET_MISSING(
      Category.USER_ERROR,
      "0038",
      "Missing option: either the '--namespace' and '--table' options or the '--control-file' option must be specified",
      "",
      ""),
  MISSING_IMPORT_FILE(
      Category.USER_ERROR,
      "0039",
      "The file '%s' specified by the argument '%s' does not exist",
      "",
      ""),
  LOG_DIRECTORY_WRITE_ACCESS_DENIED(
      Category.USER_ERROR, "0040", "Cannot write to the log directory: %s", "", ""),
  LOG_DIRECTORY_CREATION_FAILED(
      Category.USER_ERROR, "0041", "Failed to create the log directory: %s", "", ""),
  INVALID_CONTROL_FILE(Category.USER_ERROR, "0042", "Failed to parse the control file: %s", "", ""),
  DIRECTORY_WRITE_ACCESS(
      Category.USER_ERROR,
      "0043",
      "No permission to create or write files in the directory: %s",
      "",
      ""),
  DIRECTORY_CREATION_FAILED(
      Category.USER_ERROR, "0044", "Failed to create the directory: %s", "", ""),
  PATH_IS_NOT_A_DIRECTORY(
      Category.USER_ERROR, "0045", "Path exists but is not a directory: %s", "", ""),
  FILE_PATH_IS_BLANK(Category.USER_ERROR, "0046", "File path must not be blank", "", ""),
  FILE_NOT_FOUND(Category.USER_ERROR, "0047", "File not found: %s", "", ""),
  INVALID_DATE_TIME_FOR_COLUMN_VALUE(
      Category.USER_ERROR,
      "0048",
      "Invalid date/time value '%s' specified for column %s in table %s in namespace %s",
      "",
      ""),
  NULL_OR_EMPTY_KEY_VALUE_INPUT(
      Category.USER_ERROR, "0049", "Key value cannot be null or empty", "", ""),
  INVALID_KEY_VALUE_INPUT(Category.USER_ERROR, "0050", "Invalid key-value format: %s", "", ""),
  SPLIT_INPUT_VALUE_NULL(Category.USER_ERROR, "0051", "Value must not be null", "", ""),
  SPLIT_INPUT_DELIMITER_NULL(Category.USER_ERROR, "0052", "Delimiter must not be null", "", ""),
  CONFIG_FILE_PATH_BLANK(Category.USER_ERROR, "0053", "Config file path must not be blank", "", ""),
  INVALID_DATA_CHUNK_SIZE(
      Category.USER_ERROR, "0054", "Data chunk size must be greater than 0", "", ""),
  INVALID_TRANSACTION_SIZE(
      Category.USER_ERROR, "0055", "Transaction size must be greater than 0", "", ""),
  INVALID_MAX_THREADS(
      Category.USER_ERROR, "0056", "Number of max threads must be greater than 0", "", ""),
  DEPRECATED_AND_NEW_OPTION_BOTH_SPECIFIED(
      Category.USER_ERROR,
      "0057",
      "Cannot specify both deprecated option '%s' and new option '%s'. Please use only '%s'",
      "",
      ""),

  //
  // Errors for the internal error category
  //
  ERROR_CRUD_EXCEPTION(
      Category.INTERNAL_ERROR,
      "0000",
      "A problem occurred while trying to save the data. Details: %s",
      "",
      ""),
  ERROR_SCAN(
      Category.INTERNAL_ERROR,
      "0001",
      "A problem occurred while scanning. Are you sure you are running in the correct transaction mode? Details: %s",
      "",
      ""),
  CSV_FILE_READ_FAILED(
      Category.INTERNAL_ERROR, "0002", "Failed to read CSV file. Details: %s", "", ""),
  CSV_FILE_HEADER_READ_FAILED(
      Category.INTERNAL_ERROR, "0003", "Failed to CSV read header line. Details: %s", "", ""),
  DATA_CHUNK_PROCESS_FAILED(
      Category.INTERNAL_ERROR,
      "0004",
      "Data chunk processing was interrupted. Details: %s",
      "",
      ""),
  JSON_FILE_READ_FAILED(
      Category.INTERNAL_ERROR, "0005", "Failed to read JSON file. Details: %s", "", ""),
  JSONLINES_FILE_READ_FAILED(
      Category.INTERNAL_ERROR, "0006", "Failed to read JSON Lines file. Details: %s", "", ""),
  ;

  private static final String COMPONENT_NAME = "DB-DATA-LOADER";

  private final Category category;
  private final String id;
  private final String message;
  private final String cause;
  private final String solution;

  DataLoaderError(Category category, String id, String message, String cause, String solution) {
    validate(COMPONENT_NAME, category, id, message, cause, solution);

    this.category = category;
    this.id = id;
    this.message = message;
    this.cause = cause;
    this.solution = solution;
  }

  @Override
  public String getComponentName() {
    return COMPONENT_NAME;
  }

  @Override
  public Category getCategory() {
    return category;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getMessage() {
    return message;
  }

  @Override
  public String getCause() {
    return cause;
  }

  @Override
  public String getSolution() {
    return solution;
  }
}
