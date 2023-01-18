package com.scalar.db.exception.transaction;

/** An exception thrown when committing a transaction fails. */
public class CommitException extends TransactionException {

  public CommitException(String message) {
    super(message);
  }

  public CommitException(String message, Throwable cause) {
    super(message, cause);
  }
}
