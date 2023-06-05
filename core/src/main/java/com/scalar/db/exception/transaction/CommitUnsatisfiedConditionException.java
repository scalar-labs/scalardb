package com.scalar.db.exception.transaction;

/**
 * An exception thrown when one of the committed mutation fails because its condition is not
 * satisfied.
 */
public class CommitUnsatisfiedConditionException extends CommitException {

  public CommitUnsatisfiedConditionException(String message, String transactionId) {
    super(message, transactionId);
  }

  public CommitUnsatisfiedConditionException(
      String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }
}
