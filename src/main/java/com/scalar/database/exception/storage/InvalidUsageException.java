package com.scalar.database.exception.storage;

public class InvalidUsageException extends RuntimeException {

  public InvalidUsageException(String message) {
    super(message);
  }

  public InvalidUsageException(String message, Throwable cause) {
    super(message, cause);
  }
}
