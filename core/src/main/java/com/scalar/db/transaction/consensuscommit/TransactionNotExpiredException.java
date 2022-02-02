package com.scalar.db.transaction.consensuscommit;

public class TransactionNotExpiredException extends RecoveryException {

  public TransactionNotExpiredException(String message) {
    super(message);
  }

  public TransactionNotExpiredException(String message, Throwable cause) {
    super(message, cause);
  }
}
