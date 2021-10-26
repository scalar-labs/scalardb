package com.scalar.db.schemaloader.core;

import com.scalar.db.exception.storage.ExecutionException;

public class SchemaOperatorException extends ExecutionException {
  public SchemaOperatorException(String message) {
    super(message);
  }

  public SchemaOperatorException(String message, Throwable cause) {
    super(message, cause);
  }
}
