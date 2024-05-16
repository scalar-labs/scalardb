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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class tests the DirectoryValidationUtil class. */
class DirectoryValidationUtilTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(DirectoryValidationUtilTest.class);

  private Path testDirectory;

  @BeforeEach
  public void setup() throws IOException {
    testDirectory = Files.createTempDirectory("testDir");
  }

  @AfterEach
  public void cleanup() throws IOException {
    cleanUpTempDir();
  }

  @Test
  void validateTargetDirectory_ValidDirectory_NoExceptionThrown()
      throws DirectoryValidationException {
    DirectoryValidationUtil.validateTargetDirectory(testDirectory.toString());
  }

  @Test
  void validateTargetDirectory_DirectoryDoesNotExist_CreatesDirectory()
      throws DirectoryValidationException {
    Path newDirectory = Paths.get(testDirectory.toString(), "newDir");
    DirectoryValidationUtil.validateTargetDirectory(newDirectory.toString());
    assertTrue(Files.exists(newDirectory));
  }

  @Test
  void validateTargetDirectory_DirectoryNotWritable_ThrowsException() throws IOException {
    Path readOnlyDirectory =
        Files.createDirectory(Paths.get(testDirectory.toString(), "readOnlyDir"));
    readOnlyDirectory.toFile().setWritable(false);

    assertThrows(
        DirectoryValidationException.class,
        () -> {
          DirectoryValidationUtil.validateTargetDirectory(readOnlyDirectory.toString());
        });
  }

  @Test
  void validateTargetDirectory_NullDirectory_ThrowsException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          DirectoryValidationUtil.validateTargetDirectory(null);
        });
  }

  @Test
  void validateTargetDirectory_EmptyDirectory_ThrowsException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          DirectoryValidationUtil.validateTargetDirectory("");
        });
  }

  @Test
  void validateWorkingDirectory_WritableDirectory_NoExceptionThrown()
      throws DirectoryValidationException {
    DirectoryValidationUtil.validateWorkingDirectory();
  }

  @Test
  void validateWorkingDirectory_NotWritableDirectory_ThrowsException() throws IOException {
    Path readOnlyDirectory =
        Files.createDirectory(Paths.get(testDirectory.toString(), "readOnlyDir"));
    readOnlyDirectory.toFile().setWritable(false);

    System.setProperty("user.dir", readOnlyDirectory.toString());

    assertThrows(
        DirectoryValidationException.class, DirectoryValidationUtil::validateWorkingDirectory);
  }

  private void cleanUpTempDir() throws IOException {
    try (Stream<Path> paths = Files.list(testDirectory)) {
      paths.forEach(this::deleteFile);
    }
  }

  private void deleteFile(Path file) {
    try {
      Files.deleteIfExists(file);
    } catch (IOException e) {
      LOGGER.error("Failed to delete file: {}", file, e);
    }
  }
}
