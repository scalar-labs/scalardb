package com.scalar.db.storage.objectstorage;

/**
 * An exception thrown when an operation is not applied because its precondition, such as the
 * existence or the version of the object, is not satisfied.
 *
 * <p>When thrown by a write method of {@link ObjectStorageWrapper} that documents it, this
 * exception guarantees that the write has not been applied.
 */
public class PreconditionFailedException extends ObjectStorageWrapperException {

  public PreconditionFailedException(String message, Throwable cause) {
    super(message, cause);
  }

  public PreconditionFailedException(String message) {
    super(message);
  }
}
