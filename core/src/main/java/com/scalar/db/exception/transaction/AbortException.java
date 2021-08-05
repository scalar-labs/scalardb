package com.scalar.db.exception.transaction;

public class AbortException extends TransactionException {

  public AbortException(String message) {
    super(message);
  }

  public AbortException(String message, Throwable cause) {
    super(message, cause);
  }
}
