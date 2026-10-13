package com.scalar.db.transaction.consensuscommit;

public class CoordinatorConflictException extends CoordinatorException {

  // The number of times the write was sent, including the attempt that conflicted. 0 when it is not
  // recorded.
  private final int numAttempts;

  public CoordinatorConflictException(String message) {
    super(message);
    this.numAttempts = 0;
  }

  public CoordinatorConflictException(String message, int numAttempts, Throwable cause) {
    super(message, cause);
    this.numAttempts = numAttempts;
  }

  public int getNumAttempts() {
    return numAttempts;
  }
}
