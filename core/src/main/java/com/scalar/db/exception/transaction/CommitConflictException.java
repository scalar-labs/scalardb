package com.scalar.db.exception.transaction;

/**
 * An exception thrown when a transaction conflict occurs at the commit phase. You can retry the
 * transaction from the beginning in this case.
 */
public class CommitConflictException extends CommitException {

  public CommitConflictException(String message, String transactionId) {
    super(message, transactionId);
  }

  public CommitConflictException(String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }
}
