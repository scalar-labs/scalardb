package com.scalar.db.exception.transaction;

import java.util.Optional;

/** An exception thrown when the transaction status of the committing transaction is unknown. */
public class UnknownTransactionStatusException extends TransactionException {

  public UnknownTransactionStatusException(String message, String transactionId) {
    super(message, transactionId);
  }

  public UnknownTransactionStatusException(String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }

  /**
   * @return the transaction ID associated with the transaction whose status is unknown
   * @deprecated As of release 3.9.0. Will be removed in release 5.0.0
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public Optional<String> getUnknownTransactionId() {
    return getTransactionId();
  }
}
