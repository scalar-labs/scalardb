package com.scalar.db.dataloader.cli.util;

import java.nio.file.Path;
import java.nio.file.Paths;

public class FileUtils {

  /**
   * Validates the provided file path.
   *
   * @param filePath the file path to validate
   * @throws InvalidFilePathException if the file path is invalid
   */
  public static void validateFilePath(String filePath) throws InvalidFilePathException {
    Path pathToCheck = Paths.get(filePath);

    if (!pathToCheck.isAbsolute()) {
      // If the path is not absolute, it's either a file name or a relative path
      Path currentDirectory = Paths.get("").toAbsolutePath();
      Path fileInCurrentDirectory = currentDirectory.resolve(pathToCheck);

      if (!fileInCurrentDirectory.toFile().exists()) {
        throw new InvalidFilePathException("File not found: " + pathToCheck);
      }
      return;
    }

    // It's an absolute path
    if (!pathToCheck.toFile().exists()) {
      throw new InvalidFilePathException("File not found: " + pathToCheck);
    }
  }
}
