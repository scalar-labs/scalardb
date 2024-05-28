package com.scalar.db.util.groupcommit;

/** An exception thrown in a group commit operation. */
public class GroupCommitException extends RuntimeException {
  public GroupCommitException(String message) {
    super(message);
  }

  public GroupCommitException(String message, Throwable cause) {
    super(message, cause);
  }
}
