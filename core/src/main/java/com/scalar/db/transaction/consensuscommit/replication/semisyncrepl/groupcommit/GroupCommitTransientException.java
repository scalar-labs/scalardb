package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit;

public class GroupCommitTransientException extends GroupCommitException {
  public GroupCommitTransientException(String message) {
    super(message);
  }

  public GroupCommitTransientException(String message, Throwable cause) {
    super(message, cause);
  }
}
