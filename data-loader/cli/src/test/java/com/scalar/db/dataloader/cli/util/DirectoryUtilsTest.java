package com.scalar.db.dataloader.cli.util;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.scalar.db.dataloader.cli.exception.DirectoryValidationException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class tests the DirectoryValidationUtil class. */
class DirectoryUtilsTest {

  private static final Logger logger = LoggerFactory.getLogger(DirectoryUtilsTest.class);

  @TempDir Path tempDir;

  @AfterEach
  public void cleanup() throws IOException {
    cleanUpTempDir();
  }

  @Test
  void validateTargetDirectory_ValidDirectory_NoExceptionThrown()
      throws DirectoryValidationException {
    DirectoryUtils.validateOrCreateTargetDirectory(tempDir.toString());
  }

  @Test
  void validateOrCreateTargetDirectory_DirectoryDoesNotExist_CreatesDirectory()
      throws DirectoryValidationException {
    Path newDirectory = Paths.get(tempDir.toString(), "newDir");
    DirectoryUtils.validateOrCreateTargetDirectory(newDirectory.toString());
    assertTrue(Files.exists(newDirectory));
  }

  @Test
  void validateOrCreateTargetDirectory_DirectoryNotWritable_ThrowsException() throws IOException {
    Path readOnlyDirectory = Files.createDirectory(Paths.get(tempDir.toString(), "readOnlyDir"));
    readOnlyDirectory.toFile().setWritable(false);

    assertThrows(
        DirectoryValidationException.class,
        () -> {
          DirectoryUtils.validateOrCreateTargetDirectory(readOnlyDirectory.toString());
        });
  }

  @Test
  void validateTargetDirectory_NullDirectory_ThrowsException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          DirectoryUtils.validateOrCreateTargetDirectory(null);
        });
  }

  @Test
  void validateOrCreateTargetDirectory_EmptyDirectory_ThrowsException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          DirectoryUtils.validateOrCreateTargetDirectory("");
        });
  }

  private void cleanUpTempDir() throws IOException {
    try (Stream<Path> paths = Files.list(tempDir)) {
      paths.forEach(this::deleteFile);
    }
  }

  private void deleteFile(Path file) {
    try {
      Files.deleteIfExists(file);
    } catch (IOException e) {
      logger.error("Failed to delete file: {}", file, e);
    }
  }
}
