package com.scalar.database.exception.transaction;

/** */
public class CoordinatorException extends TransactionException {

  public CoordinatorException(String message) {
    super(message);
  }

  public CoordinatorException(String message, Throwable cause) {
    super(message, cause);
  }
}
