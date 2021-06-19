package com.scalar.db.exception.storage;

public class StorageRuntimeException extends RuntimeException {

  public StorageRuntimeException(String message) {
    super(message);
  }

  public StorageRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }
}
