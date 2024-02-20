package com.scalar.db.util.groupcommit;

public class GroupCommitAlreadyCompletedException extends GroupCommitException {
  public GroupCommitAlreadyCompletedException(String message) {
    super(message);
  }
}
