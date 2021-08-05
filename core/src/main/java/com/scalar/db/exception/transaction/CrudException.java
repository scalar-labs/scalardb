package com.scalar.db.exception.transaction;

public class CrudException extends TransactionException {

  public CrudException(String message) {
    super(message);
  }

  public CrudException(String message, Throwable cause) {
    super(message, cause);
  }
}
