package com.scalar.db.exception.transaction;
/** An exception thrown when a mutation fails because its condition is not satisfied. */
public class UnsatisfiedConditionException extends CrudException {

  public UnsatisfiedConditionException(String message, String transactionId) {
    super(message, transactionId);
  }
}
