package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit;

public class GroupCommitAlreadyClosedException extends GroupCommitException {
  public GroupCommitAlreadyClosedException(String message) {
    super(message);
  }

  public GroupCommitAlreadyClosedException(String message, Throwable cause) {
    super(message, cause);
  }
}
