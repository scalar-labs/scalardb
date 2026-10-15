package com.scalar.db.storage.objectstorage;

/**
 * An exception thrown when an operation fails because of a conflict with a concurrent operation.
 *
 * <p>Some write methods of {@link ObjectStorageWrapper} guarantee that the write has not been
 * applied when they throw this exception. See the documentation of each method.
 */
public class ConflictOccurredException extends ObjectStorageWrapperException {

  public ConflictOccurredException(String message, Throwable cause) {
    super(message, cause);
  }
}
