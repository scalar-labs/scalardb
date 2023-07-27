package com.scalar.db.exception.transaction;

/** An exception thrown when a mutation fails because its condition is not satisfied. */
public class UnsatisfiedConditionException extends CrudException {

  public UnsatisfiedConditionException(String message, String transactionId) {
    super(message, transactionId);
  }

  public UnsatisfiedConditionException(String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }
}
