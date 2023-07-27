package com.scalar.db.exception.transaction;

/**
 * An exception thrown when validating a transaction fails due to transient faults (e.g., a conflict
 * error). You can retry the transaction from the beginning.
 */
public class ValidationConflictException extends ValidationException {

  public ValidationConflictException(String message, String transactionId) {
    super(message, transactionId);
  }

  public ValidationConflictException(String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }
}
