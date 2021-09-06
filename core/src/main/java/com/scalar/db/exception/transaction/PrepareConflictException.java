package com.scalar.db.exception.transaction;

public class PrepareConflictException extends PrepareException {

  public PrepareConflictException(String message) {
    super(message);
  }

  public PrepareConflictException(String message, Throwable cause) {
    super(message, cause);
  }
}
