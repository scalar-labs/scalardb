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
}
