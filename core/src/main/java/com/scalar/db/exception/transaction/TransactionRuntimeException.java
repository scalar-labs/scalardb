package com.scalar.db.exception.transaction;

public class TransactionRuntimeException extends RuntimeException {

  public TransactionRuntimeException(String message) {
    super(message);
  }

  public TransactionRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }
}
