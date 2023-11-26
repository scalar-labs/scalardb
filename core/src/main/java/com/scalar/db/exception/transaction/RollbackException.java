package com.scalar.db.exception.transaction;

import javax.annotation.Nullable;

/**
 * An exception thrown when rolling back a transaction fails due to transient or nontransient
 * faults.
 */
public class RollbackException extends TransactionException {

  public RollbackException(String message, String transactionId) {
    super(message, transactionId);
  }

  public RollbackException(String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }

  public RollbackException(
      String message, @Nullable String transactionId, boolean authenticationError) {
    super(message, transactionId, authenticationError, false, false, null);
  }

  public RollbackException(
      String message,
      Throwable cause,
      @Nullable String transactionId,
      boolean authenticationError) {
    super(message, cause, transactionId, authenticationError, false, false, null);
  }
}
