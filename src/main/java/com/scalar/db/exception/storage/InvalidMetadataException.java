package com.scalar.db.exception.storage;

public class InvalidMetadataException extends RuntimeException {

  public InvalidMetadataException() {
    super("metadata doesn't have the specified column.");
  }

  public InvalidMetadataException(String message) {
    super(message);
  }
}
