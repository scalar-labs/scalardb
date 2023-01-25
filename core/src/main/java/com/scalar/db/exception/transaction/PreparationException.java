package com.scalar.db.exception.transaction;

/** An exception thrown when preparing a transaction fails. */
public class PreparationException extends TransactionException {

  public PreparationException(String message) {
    super(message);
  }

  public PreparationException(String message, Throwable cause) {
    super(message, cause);
  }
}
