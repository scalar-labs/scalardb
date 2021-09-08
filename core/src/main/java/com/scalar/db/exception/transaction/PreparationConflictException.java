package com.scalar.db.exception.transaction;

public class PreparationConflictException extends PreparationException {

  public PreparationConflictException(String message) {
    super(message);
  }

  public PreparationConflictException(String message, Throwable cause) {
    super(message, cause);
  }
}
