package com.scalar.db.exception.transaction;

/**
 * An exception thrown when a transaction CRUD operation fails due to transient or nontransient
 * faults. You can try retrying the transaction from the beginning, but the transaction may still
 * fail if the cause is nontranient.
 */
public class CrudException extends TransactionException {

  public CrudException(String message, String transactionId) {
    super(message, transactionId);
  }

  public CrudException(String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }
}
