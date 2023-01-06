package com.scalar.db.transaction.consensuscommit;

public class CoordinatorConflictException extends CoordinatorException {

  public CoordinatorConflictException(String message) {
    super(message);
  }

  public CoordinatorConflictException(String message, Throwable cause) {
    super(message, cause);
  }
}
