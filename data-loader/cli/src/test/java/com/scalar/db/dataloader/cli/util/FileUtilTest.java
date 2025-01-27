package com.scalar.db.dataloader.cli.util;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

public class FileUtilTest {

  private static final String currentPath = Paths.get("").toAbsolutePath().toString();

  @Test
  void validateFilePath_withValidFilePath_shouldNotThrowException()
      throws InvalidFilePathException {
    // Test and confirm no exception is thrown when a valid path is provided
    FileUtil.validateFilePath(currentPath);
  }

  @Test
  void validateFilePath_withInvalidFilePath_shouldThrowException() throws InvalidFilePathException {
    assertThatThrownBy(() -> FileUtil.validateFilePath(currentPath + "/demo"))
        .isInstanceOf(InvalidFilePathException.class)
        .hasMessage("File not found: " + currentPath + "/demo");
  }
}
