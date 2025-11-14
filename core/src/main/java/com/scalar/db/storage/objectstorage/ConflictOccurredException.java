package com.scalar.db.storage.objectstorage;

public class ConflictOccurredException extends ObjectStorageWrapperException {

  public ConflictOccurredException(String message, Throwable cause) {
    super(message, cause);
  }
}
