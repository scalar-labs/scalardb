package com.scalar.database.exception.transaction;

public class RequiredValueMissingException extends TransactionRuntimeException {

  public RequiredValueMissingException(String message) {
    super(message);
  }

  public RequiredValueMissingException(String message, Throwable cause) {
    super(message, cause);
  }
}
