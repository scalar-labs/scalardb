package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit;

public class GroupCommitTargetNotFoundException extends GroupCommitException {
  public GroupCommitTargetNotFoundException(String message) {
    super(message);
  }

  public GroupCommitTargetNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
}
