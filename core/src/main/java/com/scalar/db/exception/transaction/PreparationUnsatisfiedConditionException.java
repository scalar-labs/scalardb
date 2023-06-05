package com.scalar.db.exception.transaction;
/**
 * An exception thrown when one of the prepared mutation fails because its condition is not
 * satisfied.
 */
public class PreparationUnsatisfiedConditionException extends PreparationException {

  public PreparationUnsatisfiedConditionException(String message, String transactionId) {
    super(message, transactionId);
  }

  public PreparationUnsatisfiedConditionException(
      String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }
}
