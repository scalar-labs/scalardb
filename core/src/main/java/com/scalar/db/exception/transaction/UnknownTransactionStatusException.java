package com.scalar.db.exception.transaction;

import java.util.Optional;

/** An exception thrown when the transaction status of the committing transaction is unknown. */
public class UnknownTransactionStatusException extends TransactionException {
  private Optional<String> unknownTxId = Optional.empty();

  public UnknownTransactionStatusException(String message) {
    super(message);
  }

  public UnknownTransactionStatusException(String message, Throwable cause) {
    super(message, cause);
  }

  public UnknownTransactionStatusException(String message, Throwable cause, String txId) {
    super(message, cause);
    this.unknownTxId = Optional.ofNullable(txId);
  }

  public Optional<String> getUnknownTransactionId() {
    return unknownTxId;
  }
}
