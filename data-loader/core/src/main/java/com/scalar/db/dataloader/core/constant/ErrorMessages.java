package com.scalar.db.dataloader.core.constant;

public class ErrorMessages {

  public static final String ERROR_MISSING_NAMESPACE_OR_TABLE =
      "Missing namespace or table: %s, %s";
  public static final String ERROR_NUMBER_FORMAT_EXCEPTION =
      "Invalid number specified for column %s";
  public static final String ERROR_BASE64_ENCODING =
      "Invalid base64 encoding for blob value for column %s";

  private ErrorMessages() {
    // restrict instantiation
  }
}
