package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit;

public class GroupCommitCascadeException extends Exception {
  public GroupCommitCascadeException(String message, Throwable cause) {
    super(message, cause);
  }
}
