package com.scalar.db.exception.transaction;

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
}
