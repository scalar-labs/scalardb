package com.scalar.database.exception.transaction;

/** */
public class CommitConflictException extends CommitException {

  public CommitConflictException(String message) {
    super(message);
  }

  public CommitConflictException(String message, Throwable cause) {
    super(message, cause);
  }
}
