package com.scalar.db.exception.transaction;

/**
 * An exception thrown when a transaction conflict occurs during executing a CRUD operation. You can
 * retry the transaction from the beginning in this case.
 */
public class CrudConflictException extends CrudException {

  public CrudConflictException(String message, String transactionId) {
    super(message, transactionId);
  }

  public CrudConflictException(String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }
}
