package com.scalar.db.exception.transaction;

public class RollbackException extends TransactionException {

  public RollbackException(String message) {
    super(message);
  }

  public RollbackException(String message, Throwable cause) {
    super(message, cause);
  }
}
