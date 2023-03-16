package com.scalar.db.exception.transaction;

/**
 * An exception thrown when a transaction you are trying to resume is not found. You can retry the
 * transaction from the beginning in this case.
 */
public class TransactionNotFoundException extends TransactionException {

  public TransactionNotFoundException(String message, String transactionId) {
    super(message, transactionId);
  }

  public TransactionNotFoundException(String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }
}
