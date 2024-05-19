package com.scalar.db.dataloader.cli.constant;


public class ErrorMessage {

  public static final String ERROR_DIRECTORY_WRITE_ACCESS =
      "The directory '%s' does not have write permissions. Please ensure that the current user has write access to the directory.";
  public static final String ERROR_CREATE_DIRECTORY_FAILED =
      "Failed to create the directory '%s'. Please check if you have sufficient permissions and if there are any file system restrictions.";
  public static final String ERROR_EMPTY_DIRECTORY = "Directory path cannot be null or empty.";

  private ErrorMessage() {
    // restrict instantiation
  }
}
