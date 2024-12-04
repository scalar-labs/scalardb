package com.scalar.db.dataloader.core;

public class ErrorMessage {
  public static final String ERROR_MISSING_NAMESPACE_OR_TABLE =
      "the provided namespace '%s' and/or table name '%s' is incorrect and could not be found";
  public static final String ERROR_MISSING_COLUMN = "missing field or column mapping for %s";
  public static final String ERROR_MISSING_PARTITION_KEY_COLUMN =
      "missing required field or column mapping for partition key %s";
  public static final String ERROR_MISSING_CLUSTERING_KEY_COLUMN =
      "missing required field or column mapping for clustering key %s";
  public static final String ERROR_CRUD_EXCEPTION =
      "something went wrong while trying to save the data";
  public static final String ERROR_DATA_ALREADY_EXISTS = "record already exists";
  public static final String ERROR_DATA_NOT_FOUND = "record was not found";
  public static final String ERROR_CONTROL_FILE_MISSING_DATA_MAPPINGS =
      "the control file is missing data mappings";
  public static final String ERROR_TARGET_COLUMN_NOT_FOUND =
      "The target column '%s' for source field '%s' could not be found in table '%s'";
  public static final String ERROR_MISSING_PARTITION_KEY =
      "The required partition key '%s' is missing in the control file mapping for table '%s'";
  public static final String ERROR_MISSING_CLUSTERING_KEY =
      "The required clustering key '%s' is missing in the control file mapping for table '%s'";
  public static final String ERROR_MISSING_SOURCE_FIELD =
      "the data mapping source field '%s' for table '%s' is missing in the json data record";
  public static final String ERROR_DUPLICATE_DATA_MAPPINGS =
      "Duplicate data mappings found for table '%s' in the control file";
  public static final String ERROR_MISSING_COLUMN_MAPPING =
      "No mapping found for column '%s' in table '%s' in the control file. \nControl file validation set at 'FULL'. All columns need to be mapped.";
  public static final String ERROR_MULTIPLE_MAPPINGS_FOR_COLUMN_FOUND =
      "Multiple data mappings found for column '%s' in table '%s'";
  public static final String ERROR_METHOD_NULL_ARGUMENT = "Method null argument not allowed";
  public static final String ERROR_COULD_NOT_FIND_PARTITION_KEY =
      "could not find the partition key";
  public static final String ERROR_METADATA_OR_DATA_TYPES_NOT_FOUND =
      "no table meta data or a data type map was found for %s.%s";
  public static final String ERROR_EMPTY_SOURCE_ROW =
      "The source record data was undefined or empty";
  public static final String ERROR_UPSERT_INSERT_MISSING_COLUMNS =
      "The source record needs to contain all fields if the UPSERT turns into an INSERT";
  public static final String ERROR_SCAN_FAILED = "Could not complete the scan";
  public static final String ERROR_UNKNOWN_TRANSACTION_STATUS =
      "Error : the transaction to retrieve the account is in an unknown state";
  public static final String ERROR_INVALID_PROJECTION = "The column '%s' was not found";
  public static final String ERROR_SCAN =
      "Something went wrong while scanning. Are you sure you are running in the correct transaction mode?";
  public static final String ERROR_CLUSTERING_KEY_NOT_FOUND =
      "The provided clustering key %s was not found";
  public static final String ERROR_KEY_NOT_FOUND = "The key '%s' could not be found";
  public static final String ERROR_KEY_FORMATTING =
      "They provided key '%s is not formatted correctly. Expected format is field=value.";
  public static final String ERROR_SORT_FORMATTING =
      "They provided sort '%s is not formatted correctly. Expected format is field=asc|desc.";
  public static final String ERROR_VALUE_TO_STRING_CONVERSION_FAILED =
      "Something went wrong while converting the ScalarDB values to strings. The table metadata and Value datatype probably do not match.";

  public static final String ERROR_BASE64_ENCODING =
      "Invalid base64 encoding for blob value for column %s";
  public static final String ERROR_NUMBER_FORMAT_EXCEPTION =
      "Invalid number specified for column %s";
  public static final String ERROR_NULL_POINTER_EXCEPTION =
      "The %s column does not support a null value";
}
