package com.scalar.db.dataloader.core.exception;

/**
 * An exception that is thrown when an error occurs while trying to create a ScalarDB column from a
 * value
 */
public class ColumnParsingException extends Exception {

  public ColumnParsingException(String message) {
    super(message);
  }

  public ColumnParsingException(String message, Throwable cause) {
    super(message, cause);
  }
}
