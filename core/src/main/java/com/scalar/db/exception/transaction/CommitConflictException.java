package com.scalar.db.exception.transaction;

public class CommitConflictException extends CommitException {

  public CommitConflictException(String message) {
    super(message);
  }

  public CommitConflictException(String message, Throwable cause) {
    super(message, cause);
  }
}
