package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit;

public class GroupCommitCascadeException extends GroupCommitException {
  public GroupCommitCascadeException(String message) {
    super(message);
  }

  public GroupCommitCascadeException(String message, Throwable cause) {
    super(message, cause);
  }
}
