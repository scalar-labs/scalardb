package com.scalar.db.dataloader.cli.util;

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
   * Validates the provided directory path. Ensures that the directory exists and is writable. If
   * the directory doesn't exist, a creation attempt is made.
   *
   * @param directoryPath the directory path to validate
   * @throws DirectoryValidationException if the directory is not writable or cannot be created
   */
  public static void validateTargetDirectory(String directoryPath)
      throws DirectoryValidationException {
    if (StringUtils.isBlank(directoryPath)) {
      throw new IllegalArgumentException("Directory path cannot be null or empty.");
    }

    Path path = Paths.get(directoryPath);

    if (Files.exists(path)) {
      // Check if the provided directory is writable
      if (!Files.isWritable(path)) {
        throw new DirectoryValidationException(
            String.format(
                "The directory '%s' does not have write permissions. Please ensure that the current user has write access to the directory.",
                path.toAbsolutePath()));
      }

    } else {
      // Create the directory if it doesn't exist
      try {
        Files.createDirectories(path);
      } catch (IOException e) {
        throw new DirectoryValidationException(
            String.format(
                "Failed to create the directory '%s'. Please check if you have sufficient permissions and if there are any file system restrictions. Details: %s",
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
