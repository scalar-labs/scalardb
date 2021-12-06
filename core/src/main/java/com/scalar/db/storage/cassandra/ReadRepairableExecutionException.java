package com.scalar.db.storage.cassandra;

import com.scalar.db.exception.storage.ExecutionException;

public class ReadRepairableExecutionException extends ExecutionException {

  public ReadRepairableExecutionException(String message) {
    super(message);
  }

  public ReadRepairableExecutionException(String message, Throwable cause) {
    super(message, cause);
  }
}
