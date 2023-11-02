package com.scalar.db.exception.transaction;

import javax.annotation.Nullable;

/**
 * An exception thrown when aborting (rolling back) a transaction fails due to transient or
 * nontransient faults.
 */
public class AbortException extends TransactionException {

  public AbortException(String message, String transactionId) {
    super(message, transactionId);
  }

  public AbortException(String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }

  public AbortException(
      String message, @Nullable String transactionId, boolean authenticationError) {
    super(message, transactionId, authenticationError, false, false, null);
  }
}
