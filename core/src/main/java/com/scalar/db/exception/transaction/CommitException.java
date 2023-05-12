package com.scalar.db.exception.transaction;

/** An exception thrown when committing a transaction fails. */
public class CommitException extends TransactionException {

  public CommitException(String message, String transactionId) {
    super(message, transactionId);
  }

  public CommitException(String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }
}
