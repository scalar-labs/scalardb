package com.scalar.db.dataloader.cli;

public class ErrorMessage {
  public static final String ERROR_IMPORT_TARGET_MISSING =
      "Missing option: either '--namespace' and'--table' or '--control-file' options must be specified.";
  public static final String ERROR_MISSING_FILE =
      "File '%s' specified by argument '%s' does not exist.";
  public static final String ERROR_LOG_DIRECTORY_WRITE_ACCESS =
      "Not able to write to the log directory %s";
  public static final String ERROR_CREATE_LOG_DIRECTORY_FAILED =
      "Failed to create the log directory %s";
  public static final String ERROR_CONTROL_FILE_INVALID_JSON =
      "Not able to parse the %s control file";
}
