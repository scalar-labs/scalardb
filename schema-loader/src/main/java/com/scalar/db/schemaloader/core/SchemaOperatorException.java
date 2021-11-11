package com.scalar.db.schemaloader.core;

public class SchemaOperatorException extends Exception {
  public SchemaOperatorException(String message) {
    super(message);
  }

  public SchemaOperatorException(String message, Throwable cause) {
    super(message, cause);
  }
}
