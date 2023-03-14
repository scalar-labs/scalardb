package com.scalar.db.exception.transaction;

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

  public String getUnknownTransactionId() {
    return unknownTxId;
  }
}
