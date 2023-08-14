package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit;

public class GroupCommitAlreadySizeFixedException extends GroupCommitException {
  public GroupCommitAlreadySizeFixedException(String message) {
    super(message);
  }

  public GroupCommitAlreadySizeFixedException(String message, Throwable cause) {
    super(message, cause);
  }
}
