package com.scalar.db.exception.transaction;

/** An exception thrown when aborting (rolling back) a transaction fails. */
public class AbortException extends TransactionException {

  public AbortException(String message) {
    super(message);
  }

  public AbortException(String message, Throwable cause) {
    super(message, cause);
  }
}
