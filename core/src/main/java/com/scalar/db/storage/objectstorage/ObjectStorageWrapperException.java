package com.scalar.db.storage.objectstorage;

public class ObjectStorageWrapperException extends Exception {
  private final StatusCode code;

  public ObjectStorageWrapperException(StatusCode code, Throwable cause) {
    super(cause);
    this.code = code;
  }

  public StatusCode getStatusCode() {
    return code;
  }

  public enum StatusCode {
    NOT_FOUND,
    ALREADY_EXISTS,
    VERSION_MISMATCH,
  }
}
