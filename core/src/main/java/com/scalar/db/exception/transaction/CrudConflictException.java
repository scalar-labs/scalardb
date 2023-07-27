package com.scalar.db.exception.transaction;

/**
 * An exception thrown when a transaction CRUD operation fails due to transient faults (e.g., a
 * conflict error). You can retry the transaction from the beginning.
 */
public class CrudConflictException extends CrudException {

  public CrudConflictException(String message, String transactionId) {
    super(message, transactionId);
  }

  public CrudConflictException(String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }
}
