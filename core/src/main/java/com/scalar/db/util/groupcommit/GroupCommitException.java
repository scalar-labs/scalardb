package com.scalar.db.util.groupcommit;

public class GroupCommitException extends Exception {
  public GroupCommitException(String message) {
    super(message);
  }

  public GroupCommitException(String message, Throwable cause) {
    super(message, cause);
  }
}
