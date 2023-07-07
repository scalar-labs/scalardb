package com.scalar.db.exception.transaction;

/**
 * An exception thrown when preparing the transaction fails due to transient faults (e.g., a
 * conflict error). You can retry the transaction from the beginning.
 */
public class PreparationConflictException extends PreparationException {

  public PreparationConflictException(String message, String transactionId) {
    super(message, transactionId);
  }

  public PreparationConflictException(String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }
}
