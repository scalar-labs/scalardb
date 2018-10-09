package com.scalar.database.exception.storage;

public class RetriableExecutionException extends ExecutionException {

  public RetriableExecutionException(String message) {
    super(message);
  }

  public RetriableExecutionException(String message, Throwable cause) {
    super(message, cause);
  }
}
