package com.scalar.db.exception.storage;

/**
 * An exception thrown when an operation fails for a transient reason, such as a conflict with a
 * concurrent operation, and the same operation can be retried.
 *
 * <p>When this exception is thrown by a mutation operation, none of the specified mutations has
 * been applied, so retrying the operation does not apply them twice. A storage implementation must
 * not throw this exception if any of the mutations may have been applied; in that case, it must
 * throw a plain {@link ExecutionException} instead.
 */
public class RetriableExecutionException extends ExecutionException {

  public RetriableExecutionException(String message) {
    super(message);
  }

  public RetriableExecutionException(String message, Throwable cause) {
    super(message, cause);
  }
}
