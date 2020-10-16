package com.scalar.dataloader.exception;

public class DataLoaderException extends RuntimeException {
  public DataLoaderException(String message, Throwable cause) {
    super(message, cause);
  }

  public DataLoaderException(String message) {
    super(message);
  }
}
