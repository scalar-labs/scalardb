package com.scalar.db.exception.transaction;

/** An exception thrown when a CRUD operation in a transaction fails. */
public class CrudException extends TransactionException {

  public CrudException(String message, String transactionId) {
    super(message, transactionId);
  }

  public CrudException(String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }
}
