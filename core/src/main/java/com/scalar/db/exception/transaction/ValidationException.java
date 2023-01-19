package com.scalar.db.exception.transaction;

/** An exception thrown when validating a transaction fails. */
public class ValidationException extends TransactionException {

  public ValidationException(String message) {
    super(message);
  }

  public ValidationException(String message, Throwable cause) {
    super(message, cause);
  }
}
