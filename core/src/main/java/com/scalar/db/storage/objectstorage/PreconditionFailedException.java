package com.scalar.db.storage.objectstorage;

public class PreconditionFailedException extends ObjectStorageWrapperException {

  public PreconditionFailedException(String message) {
    super(message);
  }

  public PreconditionFailedException(String message, Throwable cause) {
    super(message, cause);
  }
}
