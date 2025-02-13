package com.scalar.db.dataloader.core;

public class ErrorMessage {
  public static final String INVALID_NUMBER_FORMAT_FOR_COLUMN_VALUE =
      "Invalid number specified for column %s in table %s in namespace %s";
  public static final String INVALID_BASE64_ENCODING_FOR_COLUMN_VALUE =
      "Invalid base64 encoding for blob value for column %s in table %s in namespace %s";
  public static final String INVALID_COLUMN_NON_EXISTENT =
      "Invalid key: Column %s does not exist in the table %s in namespace %s.";
  public static final String ERROR_METHOD_NULL_ARGUMENT = "Method null argument not allowed";
  public static final String PARTITION_KEY_ORDER_MISMATCH =
      "The provided partition key order does not match the table schema. Required order: %s";
  public static final String INCOMPLETE_PARTITION_KEY =
      "The provided partition key is incomplete. Required key: %s";
  public static final String CLUSTERING_KEY_ORDER_MISMATCH =
      "The provided clustering key order does not match the table schema. Required order: %s";
  public static final String CLUSTERING_KEY_NOT_FOUND =
      "The provided clustering key %s was not found";
  public static final String INVALID_PROJECTION = "The column '%s' was not found";
  public static final String ERROR_CRUD_EXCEPTION =
      "Something went wrong while trying to save the data. Details: %s";
  public static final String ERROR_SCAN =
      "Something went wrong while scanning. Are you sure you are running in the correct transaction mode? Details: %s";
  public static final String MISSING_NAMESPACE_OR_TABLE = "Missing namespace or table: %s, %s";
  public static final String TABLE_METADATA_RETRIEVAL_FAILED =
      "Failed to retrieve table metadata. Details: %s";
  public static final String DUPLICATE_DATA_MAPPINGS =
      "Duplicate data mappings found for table '%s' in the control file";
  public static final String MISSING_COLUMN_MAPPING =
      "No mapping found for column '%s' in table '%s' in the control file. Control file validation set at 'FULL'. All columns need to be mapped.";
  public static final String CONTROL_FILE_MISSING_DATA_MAPPINGS =
      "The control file is missing data mappings";
  public static final String TARGET_COLUMN_NOT_FOUND =
      "The target column '%s' for source field '%s' could not be found in table '%s'";
  public static final String MISSING_PARTITION_KEY =
      "The required partition key '%s' is missing in the control file mapping for table '%s'";
  public static final String MISSING_CLUSTERING_KEY =
      "The required clustering key '%s' is missing in the control file mapping for table '%s'";
  public static final String MULTIPLE_MAPPINGS_FOR_COLUMN_FOUND =
      "Duplicated data mappings found for column '%s' in table '%s'";
  public static final String MISSING_CLUSTERING_KEY_COLUMN =
      "Missing required field or column mapping for clustering key %s";
  public static final String MISSING_PARTITION_KEY_COLUMN =
      "Missing required field or column mapping for partition key %s";
  public static final String MISSING_COLUMN = "Missing field or column mapping for %s";
}
