package com.scalar.db.util.groupcommit;

// TODO: Remove this if possible
/** An exception thrown when the group is emitted. */
public class GroupCommitAlreadyCompletedException extends GroupCommitException {
  public GroupCommitAlreadyCompletedException(String message) {
    super(message);
  }
}
