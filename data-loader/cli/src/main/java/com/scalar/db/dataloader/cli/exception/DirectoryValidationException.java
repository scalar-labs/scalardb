package com.scalar.db.dataloader.cli.exception;

/** Exception thrown when there is an error validating a directory. */
public class DirectoryValidationException extends Exception {

  public DirectoryValidationException(String message) {
    super(message);
  }
}
