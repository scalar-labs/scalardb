package com.scalar.db.dataloader.cli.util;

import com.scalar.db.dataloader.cli.exception.InvalidFilePathException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FileUtil {

  /**
   * Validates the provided file path.
   *
   * @param filePath the file path to validate
   * @throws InvalidFilePathException if the file path is invalid or the file does not exist
   */
  public static void validateFilePath(String filePath) throws InvalidFilePathException {
    if (StringUtils.isBlank(filePath)) {
      throw new InvalidFilePathException("File path cannot be null or empty.");
    }
    Path path = Paths.get(filePath);

    if (!Files.exists(path)) {
      throw new InvalidFilePathException("File not found: " + filePath);
    }
  }
}
