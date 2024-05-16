package com.scalar.db.dataloader.cli.util;

import static org.junit.jupiter.api.Assertions.*;

import com.scalar.db.dataloader.cli.exception.InvalidFilePathException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** This class tests the FileUtil class. */
class FileUtilTest {

  @TempDir Path tempDir;

  Path existingFile;
  Path nonExistingFile;

  @BeforeEach
  void setUp() throws IOException {
    existingFile = tempDir.resolve("existing_file.txt");
    Files.createFile(existingFile);

    nonExistingFile = tempDir.resolve("non_existing_file.txt");
  }

  @Test
  void validateFilePath_WithExistingFile_ShouldNotThrowException() {
    assertDoesNotThrow(() -> FileUtil.validateFilePath(existingFile.toString()));
  }

  @Test
  void validateFilePath_WithNonExistingFile_ShouldThrowInvalidFilePathException() {
    assertThrows(
        InvalidFilePathException.class,
        () -> FileUtil.validateFilePath(nonExistingFile.toString()));
  }

  @Test
  void validateFilePath_WithNullFilePath_ShouldThrowNullPointerException() {
    assertThrows(InvalidFilePathException.class, () -> FileUtil.validateFilePath(null));
  }

  @Test
  void validateFilePath_WithEmptyFilePath_ShouldThrowInvalidFilePathException() {
    assertThrows(InvalidFilePathException.class, () -> FileUtil.validateFilePath(""));
  }

  @Test
  void validateFilePath_WithAbsolutePath_ShouldNotThrowException() {
    assertDoesNotThrow(() -> FileUtil.validateFilePath(existingFile.toAbsolutePath().toString()));
  }

  @Test
  void validateFilePath_WithRelativePath_ShouldNotThrowException() throws IOException {
    Path currentDirectory = Paths.get("").toAbsolutePath();
    Path relativeExistingFile = currentDirectory.resolve("relative_existing_file.txt");
    Files.createFile(relativeExistingFile);

    assertDoesNotThrow(
        () -> FileUtil.validateFilePath(relativeExistingFile.getFileName().toString()));

    // Clean up the created file after the test
    Files.delete(relativeExistingFile);
  }
}
