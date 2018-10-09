package com.scalar.database.exception.storage;

public class UnsupportedTypeException extends RuntimeException {

  public UnsupportedTypeException() {
    super("unsupported type is specified.");
  }

  public UnsupportedTypeException(String message) {
    super(message);
  }
}
