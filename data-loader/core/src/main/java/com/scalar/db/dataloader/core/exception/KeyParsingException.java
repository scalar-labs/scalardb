package com.scalar.db.dataloader.core.exception;

/**
 * An exception that is thrown when an error occurs while trying to create a ScalarDB key from a
 * value.
 *
 * <p>This exception is typically used to indicate a problem with parsing or converting data into a
 * format that can be used to create a key in ScalarDB.
 */
public class KeyParsingException extends Exception {

  /**
   * Constructs a new {@code KeyParsingException} with the specified detail message.
   *
   * @param message the detail message explaining the cause of the exception
   */
  public KeyParsingException(String message) {
    super(message);
  }

  /**
   * Constructs a new {@code KeyParsingException} with the specified detail message and cause.
   *
   * @param message the detail message explaining the cause of the exception
   * @param cause the cause of the exception (can be {@code null})
   */
  public KeyParsingException(String message, Throwable cause) {
    super(message, cause);
  }
}
