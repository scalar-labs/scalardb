package com.scalar.db.exception.transaction;

/**
 * An exception thrown when a transaction conflict occurs at the commit phase. You can retry the
 * same transaction from the beginning in this case.
 */
public class CommitConflictException extends CommitException {

  public CommitConflictException(String message) {
    super(message);
  }

  public CommitConflictException(String message, Throwable cause) {
    super(message, cause);
  }
}
