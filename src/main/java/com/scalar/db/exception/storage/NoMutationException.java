package com.scalar.db.exception.storage;

public class NoMutationException extends ExecutionException {

  public NoMutationException(String message) {
    super(message);
  }

  public NoMutationException(String message, Throwable cause) {
    super(message, cause);
  }
}
