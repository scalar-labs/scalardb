package com.scalar.db.exception.storage;

public class ExecutionException extends Exception {

  public ExecutionException(String message) {
    super(message);
  }

  public ExecutionException(String message, Throwable cause) {
    super(message, cause);
  }
}
