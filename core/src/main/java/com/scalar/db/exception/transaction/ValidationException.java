package com.scalar.db.exception.transaction;

import javax.annotation.Nullable;

/**
 * An exception thrown when validating a transaction fails due to transient or nontransient faults.
 * You can try retrying the transaction from the beginning, but the transaction may still fail if
 * the cause is nontranient.
 */
public class ValidationException extends TransactionException {

  public ValidationException(String message, String transactionId) {
    super(message, transactionId);
  }

  public ValidationException(String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }

  public ValidationException(
      String message, @Nullable String transactionId, boolean authenticationError) {
    super(message, transactionId, authenticationError, false, false, null);
  }

  public ValidationException(
      String message,
      Throwable cause,
      @Nullable String transactionId,
      boolean authenticationError) {
    super(message, cause, transactionId, authenticationError, false, false, null);
  }
}
