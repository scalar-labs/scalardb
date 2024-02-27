package com.scalar.db.util.groupcommit;

/** An exception thrown when the group is already closed (== size-fixed). */
public class GroupCommitAlreadyClosedException extends GroupCommitException {
  public GroupCommitAlreadyClosedException(String message) {
    super(message);
  }
}
