package com.scalar.db.sql.exception;

public class TransactionConflictException extends SqlException {
  public TransactionConflictException() {
    super();
  }

  public TransactionConflictException(String message) {
    super(message);
  }

  public TransactionConflictException(String message, Throwable cause) {
    super(message, cause);
  }

  public TransactionConflictException(Throwable cause) {
    super(cause);
  }
}
