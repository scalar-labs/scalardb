package com.scalar.db.exception.transaction;

/**
 * An exception thrown when committing a transaction fails due to transient faults (e.g., a conflict
 * error). You can retry the transaction from the beginning.
 */
public class CommitConflictException extends CommitException {

  public CommitConflictException(String message, String transactionId) {
    super(message, transactionId);
  }

  public CommitConflictException(String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }
}
