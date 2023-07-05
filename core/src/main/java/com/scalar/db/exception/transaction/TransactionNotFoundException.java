package com.scalar.db.exception.transaction;

/**
 * An exception thrown when a transaction you are trying to resume is not found or
 * beginning/starting/joining a transaction fails due to transient faults. You can retry the
 * transaction from the beginning.
 */
public class TransactionNotFoundException extends TransactionException {

  public TransactionNotFoundException(String message, String transactionId) {
    super(message, transactionId);
  }

  public TransactionNotFoundException(String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }
}
