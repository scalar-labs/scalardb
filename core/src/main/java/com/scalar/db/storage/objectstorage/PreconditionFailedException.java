package com.scalar.db.storage.objectstorage;

public class PreconditionFailedException extends ObjectStorageWrapperException {

  public PreconditionFailedException(String message, Throwable cause) {
    super(message, cause);
  }

  public PreconditionFailedException(String message) {
    super(message);
  }
}
