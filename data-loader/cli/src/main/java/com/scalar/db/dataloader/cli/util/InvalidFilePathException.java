package com.scalar.db.dataloader.cli.util;

public class InvalidFilePathException extends Exception {

  public InvalidFilePathException(String message) {
    super(message);
  }

  public InvalidFilePathException(String message, Throwable cause) {
    super(message, cause);
  }
}
