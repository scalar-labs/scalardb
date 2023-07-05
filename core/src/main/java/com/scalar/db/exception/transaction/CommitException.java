package com.scalar.db.exception.transaction;

/**
 * An exception thrown when committing a transaction fails due to transient or nontransient faults.
 * You can try retrying the transaction from the beginning, but you may not be able to retry the
 * transaction due to nontransient faults.
 */
public class CommitException extends TransactionException {

  public CommitException(String message, String transactionId) {
    super(message, transactionId);
  }

  public CommitException(String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }
}
