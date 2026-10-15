package com.scalar.db.storage.objectstorage;

/**
 * An exception thrown when the precondition of an operation, such as the existence or the version
 * of the object, is not satisfied.
 *
 * <p>Some write methods of {@link ObjectStorageWrapper} guarantee that the write has not been
 * applied when they throw this exception. See the documentation of each method.
 */
public class PreconditionFailedException extends ObjectStorageWrapperException {

  public PreconditionFailedException(String message, Throwable cause) {
    super(message, cause);
  }

  public PreconditionFailedException(String message) {
    super(message);
  }
}
