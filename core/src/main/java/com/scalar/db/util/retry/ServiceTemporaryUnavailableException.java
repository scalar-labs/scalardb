package com.scalar.db.util.retry;

public class ServiceTemporaryUnavailableException extends RuntimeException {

  public ServiceTemporaryUnavailableException() {
    super();
  }

  public ServiceTemporaryUnavailableException(String message) {
    super(message);
  }

  public ServiceTemporaryUnavailableException(String message, Throwable cause) {
    super(message, cause);
  }

  public ServiceTemporaryUnavailableException(Throwable cause) {
    super(cause);
  }
}
