package com.scalar.db.exception.transaction;

/**
 * An exception thrown when a transaction conflict occurs at the prepare phase. You can retry the
 * same transaction from the beginning in this case.
 */
public class PreparationConflictException extends PreparationException {

  public PreparationConflictException(String message) {
    super(message);
  }

  public PreparationConflictException(String message, Throwable cause) {
    super(message, cause);
  }
}
