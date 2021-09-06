package com.scalar.db.exception.transaction;

public class PrepareException extends TransactionException {

  public PrepareException(String message) {
    super(message);
  }

  public PrepareException(String message, Throwable cause) {
    super(message, cause);
  }
}
