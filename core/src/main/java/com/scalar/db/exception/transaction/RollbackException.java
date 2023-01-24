package com.scalar.db.exception.transaction;

/** An exception thrown when rolling back a transaction fails. */
public class RollbackException extends TransactionException {

  public RollbackException(String message) {
    super(message);
  }

  public RollbackException(String message, Throwable cause) {
    super(message, cause);
  }
}
