package com.scalar.db.util.groupcommit;

public class GroupCommitAlreadyClosedException extends GroupCommitException {
  public GroupCommitAlreadyClosedException(String message) {
    super(message);
  }
}
