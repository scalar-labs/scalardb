package com.scalar.db.transaction.consensuscommit;

public class CoordinatorException extends Exception {

  public CoordinatorException(String message) {
    super(message);
  }

  public CoordinatorException(String message, Throwable cause) {
    super(message, cause);
  }
}
