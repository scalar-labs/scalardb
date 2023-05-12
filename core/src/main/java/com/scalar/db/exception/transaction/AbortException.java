package com.scalar.db.exception.transaction;

/** An exception thrown when aborting (rolling back) a transaction fails. */
public class AbortException extends TransactionException {

  public AbortException(String message, String transactionId) {
    super(message, transactionId);
  }

  public AbortException(String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }
}
