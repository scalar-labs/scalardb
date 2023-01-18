package com.scalar.db.exception.transaction;

/**
 * An exception thrown when something retriable situation happens. You can retry the same
 * transaction from the beginning in this case.
 */
public class RetriableTransactionException extends TransactionException {
  public RetriableTransactionException(String message) {
    super(message);
  }

  public RetriableTransactionException(String message, Throwable cause) {
    super(message, cause);
  }
}
