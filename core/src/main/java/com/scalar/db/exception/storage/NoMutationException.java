package com.scalar.db.exception.storage;

/**
 * An exception thrown when a mutation operation is not applied because a condition specified in the
 * mutations is not satisfied.
 *
 * <p>When this exception is thrown, none of the specified mutations has been applied. A storage
 * implementation must not throw this exception if any of the mutations may have been applied; in
 * that case, it must throw a plain {@link ExecutionException} instead.
 */
public class NoMutationException extends ExecutionException {

  public NoMutationException(String message) {
    super(message);
  }

  public NoMutationException(String message, Throwable cause) {
    super(message, cause);
  }
}
