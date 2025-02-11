package com.scalar.db.dataloader.cli.util;

import static com.scalar.db.dataloader.cli.ErrorMessage.ERROR_DIRECTORY_WRITE_ACCESS;

import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.cli.exception.DirectoryValidationException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.lang3.StringUtils;

/** Utility class for validating and handling directories. */
public final class DirectoryUtils {

  private DirectoryUtils() {
    // restrict instantiation
  }

  /**
   * Validates the current working directory. Ensures that it is writable.
   *
   * @throws DirectoryValidationException if the current working directory is not writable
   */
  public static void validateWorkingDirectory() throws DirectoryValidationException {
    Path workingDirectoryPath = Paths.get(System.getProperty("user.dir"));

    // Check if the current working directory is writable
    if (!Files.isWritable(workingDirectoryPath)) {
      throw new DirectoryValidationException(
          String.format(ERROR_DIRECTORY_WRITE_ACCESS, workingDirectoryPath.toAbsolutePath()));
    }
  }

  /**
   * Validates the provided directory path. Ensures that the directory exists and is writable. If
   * the directory doesn't exist, a creation attempt is made.
   *
   * @param directoryPath the directory path to validate
   * @throws DirectoryValidationException if the directory is not writable or cannot be created
   */
  public static void validateTargetDirectory(String directoryPath)
      throws DirectoryValidationException {
    if (StringUtils.isBlank(directoryPath)) {
      throw new IllegalArgumentException(
          CoreError.DATA_LOADER_MISSING_DIRECTORY_NOT_ALLOWED.buildMessage());
    }

    Path path = Paths.get(directoryPath);

    if (Files.exists(path)) {
      // Check if the provided directory is writable
      if (!Files.isWritable(path)) {
        throw new DirectoryValidationException(
            CoreError.DATA_LOADER_DIRECTORY_WRITE_ACCESS_NOT_ALLOWED.buildMessage(
                path.toAbsolutePath()));
      }

    } else {
      // Create the directory if it doesn't exist
      try {
        Files.createDirectories(path);
      } catch (IOException e) {
        throw new DirectoryValidationException(
            CoreError.DATA_LOADER_DIRECTORY_CREATE_FAILED.buildMessage(
                path.toAbsolutePath(), e.getMessage()));
      }
    }
  }

  /**
   * Returns the current working directory.
   *
   * @return the current working directory
   */
  public static String getCurrentWorkingDirectory() {
    return Paths.get(System.getProperty("user.dir")).toAbsolutePath().toString();
  }
}
