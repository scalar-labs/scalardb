package com.scalar.db.exception.transaction;

import java.util.Optional;
import javax.annotation.Nullable;

/** Base class for all exceptions thrown by the Transaction API. */
public class TransactionException extends Exception {

  @Nullable private final String transactionId;

  public TransactionException(String message, @Nullable String transactionId) {
    super(message);
    this.transactionId = transactionId;
  }

  public TransactionException(String message, Throwable cause, @Nullable String transactionId) {
    super(message, cause);
    this.transactionId = transactionId;
  }

  /**
   * Returns a transaction ID that associated with the transaction whose status is unknown.
   *
   * @return a transaction ID that associated with the transaction whose status is unknown
   */
  public Optional<String> getTransactionId() {
    return Optional.ofNullable(transactionId);
  }
}
