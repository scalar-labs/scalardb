package com.scalar.database.exception.transaction;

public class CrudRuntimeException extends TransactionRuntimeException {

  public CrudRuntimeException(String message) {
    super(message);
  }

  public CrudRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }
}
