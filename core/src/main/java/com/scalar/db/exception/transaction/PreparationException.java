package com.scalar.db.exception.transaction;

import javax.annotation.Nullable;

/**
 * An exception thrown when preparing a transaction fails due to transient or nontransient faults.
 * You can try retrying the transaction from the beginning, but the transaction may still fail if
 * the cause is nontransient.
 */
public class PreparationException extends TransactionException {

  public PreparationException(String message, String transactionId) {
    super(message, transactionId);
  }

  public PreparationException(String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }

  public PreparationException(
      String message, @Nullable String transactionId, boolean authenticationError) {
    super(message, transactionId, authenticationError, false, false, null);
  }

  public PreparationException(
      String message,
      Throwable cause,
      @Nullable String transactionId,
      boolean authenticationError) {
    super(message, cause, transactionId, authenticationError, false, false, null);
  }
}
