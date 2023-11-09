package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.client;

public class GroupCommitException extends Exception {
  public GroupCommitException(String message, Throwable cause) {
    super(message, cause);
  }
}
