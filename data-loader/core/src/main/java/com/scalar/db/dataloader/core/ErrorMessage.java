package com.scalar.db.dataloader.core;

public class ErrorMessage {
  public static final String INVALID_NUMBER_FORMAT_FOR_COLUMN_VALUE =
      "Invalid number specified for column %s in table %s in namespace %s";
  public static final String INVALID_BASE64_ENCODING_FOR_COLUMN_VALUE =
      "Invalid base64 encoding for blob value for column %s in table %s in namespace %s";
  public static final String INVALID_COLUMN_NON_EXISTENT =
      "Invalid key: Column %s does not exist in the table %s in namespace %s.";
  public static final String ERROR_METHOD_NULL_ARGUMENT = "Method null argument not allowed";
}
