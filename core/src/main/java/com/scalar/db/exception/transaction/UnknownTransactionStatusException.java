package com.scalar.db.exception.transaction;

import java.util.Optional;

/** An exception thrown when the transaction status of the committing transaction is unknown. */
public class UnknownTransactionStatusException extends TransactionException {
  private final String unknownTxId;

  public UnknownTransactionStatusException(String message, String txId) {
    super(message);
    this.unknownTxId = txId;
  }

  public UnknownTransactionStatusException(String message, Throwable cause, String txId) {
    super(message, cause);
    this.unknownTxId = txId;
  }

  /**
   * @return a transaction ID associated with the transaction whose status is unknown
   * @deprecated As of release 3.9.0. Will be removed in release 5.0.0
   */
  @Deprecated
  public Optional<String> getUnknownTransactionId() {
    return Optional.of(unknownTxId);
  }

  /**
   * Returns a transaction ID associated with the transaction whose status is unknown.
   *
   * @return a transaction ID associated with the transaction whose status is unknown
   */
  public String getTransactionId() {
    return unknownTxId;
  }
}
