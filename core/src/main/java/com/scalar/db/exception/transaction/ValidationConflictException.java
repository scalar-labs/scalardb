package com.scalar.db.exception.transaction;

/**
 * An exception thrown when a transaction conflict occurs at the validation phase. You can retry the
 * transaction from the beginning in this case.
 */
public class ValidationConflictException extends ValidationException {

  public ValidationConflictException(String message, String transactionId) {
    super(message, transactionId);
  }

  public ValidationConflictException(String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }
}
