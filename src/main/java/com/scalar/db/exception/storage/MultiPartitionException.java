package com.scalar.db.exception.storage;

public class MultiPartitionException extends IllegalArgumentException {

  public MultiPartitionException(String message) {
    super(message);
  }

  public MultiPartitionException(String message, Throwable cause) {
    super(message, cause);
  }
}
