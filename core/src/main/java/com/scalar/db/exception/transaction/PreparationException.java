package com.scalar.db.exception.transaction;

/**
 * An exception thrown when preparing a transaction fails due to transient or nontransient faults.
 * You can try retrying the transaction from the beginning, but you may not be able to retry the
 * transaction due to nontransient faults.
 */
public class PreparationException extends TransactionException {

  public PreparationException(String message, String transactionId) {
    super(message, transactionId);
  }

  public PreparationException(String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }
}
