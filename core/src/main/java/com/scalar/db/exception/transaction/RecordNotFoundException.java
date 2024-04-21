package com.scalar.db.exception.transaction;

import com.scalar.db.api.Update;

/**
 * An exception thrown when no condition is specified in the {@link Update} command and the entry
 * does not exist.
 */
public class RecordNotFoundException extends CrudException {

  public RecordNotFoundException(String message, String transactionId) {
    super(message, transactionId);
  }

  public RecordNotFoundException(String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }
}
