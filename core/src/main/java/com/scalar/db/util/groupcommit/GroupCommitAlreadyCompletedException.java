package com.scalar.db.util.groupcommit;

/** An exception thrown when the group is emitted. */
public class GroupCommitAlreadyCompletedException extends GroupCommitException {
  public GroupCommitAlreadyCompletedException(String message) {
    super(message);
  }
}
