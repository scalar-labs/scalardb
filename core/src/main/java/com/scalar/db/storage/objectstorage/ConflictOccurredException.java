package com.scalar.db.storage.objectstorage;

/**
 * An exception thrown when an operation fails because of a conflict with a concurrent operation.
 *
 * <p>When thrown by a write method of {@link ObjectStorageWrapper} that documents it, this
 * exception guarantees that the write has not been applied.
 */
public class ConflictOccurredException extends ObjectStorageWrapperException {

  public ConflictOccurredException(String message, Throwable cause) {
    super(message, cause);
  }
}
