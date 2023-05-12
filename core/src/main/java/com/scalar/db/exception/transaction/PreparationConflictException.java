package com.scalar.db.exception.transaction;

/**
 * An exception thrown when a transaction conflict occurs at the prepare phase. You can retry the
 * transaction from the beginning in this case.
 */
public class PreparationConflictException extends PreparationException {

  public PreparationConflictException(String message, String transactionId) {
    super(message, transactionId);
  }

  public PreparationConflictException(String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }
}
