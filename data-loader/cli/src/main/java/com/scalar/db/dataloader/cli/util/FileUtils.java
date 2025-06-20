package com.scalar.db.dataloader.cli.util;

import com.scalar.db.dataloader.core.DataLoaderError;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.lang3.StringUtils;

public class FileUtils {

  /**
   * Validates the provided file path.
   *
   * @param filePath the file path to validate
   * @throws InvalidFilePathException if the file path is invalid
   */
  public static void validateFilePath(String filePath) throws InvalidFilePathException {
    if (StringUtils.isBlank(filePath)) {
      throw new IllegalArgumentException(DataLoaderError.FILE_PATH_IS_BLANK.buildMessage());
    }
    Path pathToCheck = Paths.get(filePath);

    if (!pathToCheck.toFile().exists()) {
      throw new InvalidFilePathException(DataLoaderError.FILE_NOT_FOUND.buildMessage(pathToCheck));
    }
  }
}
