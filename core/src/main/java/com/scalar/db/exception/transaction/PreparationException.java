package com.scalar.db.exception.transaction;

public class PreparationException extends TransactionException {

  public PreparationException(String message) {
    super(message);
  }

  public PreparationException(String message, Throwable cause) {
    super(message, cause);
  }
}
