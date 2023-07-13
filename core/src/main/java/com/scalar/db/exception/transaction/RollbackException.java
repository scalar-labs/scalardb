package com.scalar.db.exception.transaction;

/**
 * An exception thrown when rolling back a transaction fails due to transient or nontransient
 * faults.
 */
public class RollbackException extends TransactionException {

  public RollbackException(String message, String transactionId) {
    super(message, transactionId);
  }

  public RollbackException(String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }
}
