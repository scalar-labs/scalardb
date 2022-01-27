package com.scalar.db.transaction.consensuscommit;

public class RecoveryException extends Exception {

  public RecoveryException(String message) {
    super(message);
  }

  public RecoveryException(String message, Throwable cause) {
    super(message, cause);
  }
}
