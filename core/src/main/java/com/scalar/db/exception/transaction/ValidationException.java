package com.scalar.db.exception.transaction;

/** An exception thrown when validating a transaction fails. */
public class ValidationException extends TransactionException {

  public ValidationException(String message, String transactionId) {
    super(message, transactionId);
  }

  public ValidationException(String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }
}
