package com.scalar.db.exception.transaction;

/**
 * An exception thrown when a transaction conflict occurs at the validation phase. You can retry the
 * transaction from the beginning in this case.
 */
public class ValidationConflictException extends ValidationException {

  public ValidationConflictException(String message) {
    super(message);
  }

  public ValidationConflictException(String message, Throwable cause) {
    super(message, cause);
  }
}
