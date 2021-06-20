package com.scalar.db.exception.transaction;

public class CommitRuntimeException extends TransactionRuntimeException {

  public CommitRuntimeException(String message) {
    super(message);
  }

  public CommitRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }
}
