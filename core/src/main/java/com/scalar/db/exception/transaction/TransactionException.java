package com.scalar.db.exception.transaction;

import java.util.Optional;
import javax.annotation.Nullable;

/** Base class for all exceptions thrown by the Transaction API. */
public class TransactionException extends Exception {

  @Nullable private final String transactionId;

  public TransactionException(String message, @Nullable String transactionId) {
    super(addTransactionIdToMessage(message, transactionId));
    this.transactionId = transactionId;
  }

  public TransactionException(String message, Throwable cause, @Nullable String transactionId) {
    super(addTransactionIdToMessage(message, transactionId), cause);
    this.transactionId = transactionId;
  }

  /**
   * Returns the transaction ID associated with the transaction that threw the exception.
   *
   * @return the transaction ID associated with the transaction that threw the exception
   */
  public Optional<String> getTransactionId() {
    return Optional.ofNullable(transactionId);
  }

  private static String addTransactionIdToMessage(String message, @Nullable String transactionId) {
    if (transactionId == null) {
      return message;
    }

    String suffix = ". transaction ID: " + transactionId;

    // To avoid a duplicated transaction ID, check if the message already has a transaction ID
    if (!message.endsWith(suffix)) {
      return message + suffix;
    } else {
      return message;
    }
  }
}
