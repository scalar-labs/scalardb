package com.scalar.db.sql.exception;

public class UnknownTransactionStatusException extends SqlException {
  public UnknownTransactionStatusException() {
    super();
  }

  public UnknownTransactionStatusException(String message) {
    super(message);
  }

  public UnknownTransactionStatusException(String message, Throwable cause) {
    super(message, cause);
  }

  public UnknownTransactionStatusException(Throwable cause) {
    super(cause);
  }
}
