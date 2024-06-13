package com.scalar.db.dataloader.core.exception;

/**
 * An exception that is thrown when an error occurs while trying to create a ScalarDB from a value
 */
public class KeyParsingException extends Exception {

  public KeyParsingException(String message) {
    super(message);
  }

  public KeyParsingException(String message, Throwable cause) {
    super(message, cause);
  }
}
