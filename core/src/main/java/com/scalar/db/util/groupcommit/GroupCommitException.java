package com.scalar.db.util.groupcommit;

/** An exception thrown related to group commit. */
public class GroupCommitException extends Exception {
  public GroupCommitException(String message) {
    super(message);
  }

  public GroupCommitException(String message, Throwable cause) {
    super(message, cause);
  }
}
