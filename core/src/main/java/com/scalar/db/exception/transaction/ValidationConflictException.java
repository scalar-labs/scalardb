package com.scalar.db.exception.transaction;

public class ValidationConflictException extends ValidationException {

  public ValidationConflictException(String message) {
    super(message);
  }

  public ValidationConflictException(String message, Throwable cause) {
    super(message, cause);
  }
}
