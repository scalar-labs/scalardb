package com.scalar.db.exception.storage;

public class ConnectionException extends RuntimeException {

  public ConnectionException(String message) {
    super(message);
  }

  public ConnectionException(String message, Throwable cause) {
    super(message, cause);
  }
}
