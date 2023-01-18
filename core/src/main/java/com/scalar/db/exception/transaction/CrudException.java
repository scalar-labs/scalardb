package com.scalar.db.exception.transaction;

/** An exception thrown when a CRUD operation in a transaction fails. */
public class CrudException extends TransactionException {

  public CrudException(String message) {
    super(message);
  }

  public CrudException(String message, Throwable cause) {
    super(message, cause);
  }
}
