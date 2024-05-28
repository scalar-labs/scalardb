package com.scalar.db.util.groupcommit;

/**
 * An exception thrown when the current state of a target group or slot does not match the expected
 * state in a group commit operation.
 */
public class GroupCommitConflictException extends GroupCommitException {
  public GroupCommitConflictException(String message) {
    super(message);
  }
}
