package com.scalar.db.util.groupcommit;

/** An exception thrown when the group commit fails */
class GroupCommitFailureException extends GroupCommitException {
  public GroupCommitFailureException(String message, Exception cause) {
    super(message, cause);
  }
}
