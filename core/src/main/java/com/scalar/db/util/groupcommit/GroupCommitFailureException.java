package com.scalar.db.util.groupcommit;

public class GroupCommitFailureException extends GroupCommitException {
  public GroupCommitFailureException(String message) {
    super(message);
  }

  public GroupCommitFailureException(String message, Exception cause) {
    super(message, cause);
  }
}
