package com.scalar.db.schemaloader;

public class SchemaLoaderException extends Exception {

  public SchemaLoaderException(String message) {
    super(message);
  }

  public SchemaLoaderException(String message, Throwable cause) {
    super(message, cause);
  }
}
