package com.scalar.db.util.groupcommit;

/** An exception thrown when the target slot is not found. */
class GroupCommitTargetNotFoundException extends GroupCommitException {
  public GroupCommitTargetNotFoundException(String message) {
    super(message);
  }
}
