package com.scalar.db.exception.transaction;

/** An exception thrown when preparing a transaction fails. */
public class PreparationException extends TransactionException {

  public PreparationException(String message, String transactionId) {
    super(message, transactionId);
  }

  public PreparationException(String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }
}
