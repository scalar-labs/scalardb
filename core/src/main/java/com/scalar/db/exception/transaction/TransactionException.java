package com.scalar.db.exception.transaction;

/** Base class for all exceptions thrown by the Transaction API. */
public class TransactionException extends Exception {

  public TransactionException(String message) {
    super(message);
  }

  public TransactionException(String message, Throwable cause) {
    super(message, cause);
  }
}
