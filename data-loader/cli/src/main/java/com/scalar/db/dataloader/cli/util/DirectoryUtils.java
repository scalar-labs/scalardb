package com.scalar.db.dataloader.cli.util;

import static com.scalar.db.dataloader.cli.constant.ErrorMessage.ERROR_CREATE_DIRECTORY_FAILED;
import static com.scalar.db.dataloader.cli.constant.ErrorMessage.ERROR_DIRECTORY_WRITE_ACCESS;
import static com.scalar.db.dataloader.cli.constant.ErrorMessage.ERROR_EMPTY_DIRECTORY;

import com.scalar.db.dataloader.cli.exception.DirectoryValidationException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.lang3.StringUtils;

/** Utility class for validating and handling directories. */
public class DirectoryUtils {

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
      throw new IllegalArgumentException(ERROR_EMPTY_DIRECTORY);
    }

    Path path = Paths.get(directoryPath);

    if (Files.exists(path)) {
      // Check if the provided directory is writable
      if (!Files.isWritable(path)) {
        throw new DirectoryValidationException(
            String.format(ERROR_DIRECTORY_WRITE_ACCESS, path.toAbsolutePath()));
      }
    } else {
      // Create the directory if it doesn't exist
      try {
        Files.createDirectories(path);
      } catch (IOException e) {
        throw new DirectoryValidationException(
            String.format(ERROR_CREATE_DIRECTORY_FAILED, path.toAbsolutePath()));
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
