package com.scalar.db.dataloader.cli.command.dataexport;

import static org.junit.jupiter.api.Assertions.*;

import com.scalar.db.dataloader.cli.exception.InvalidFileExtensionException;
import com.scalar.db.service.StorageFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

class ExportCommandTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExportCommandTest.class);
  @TempDir Path tempDir;
  @Mock StorageFactory storageFactory;

  private ExportCommand exportCommand;

  @BeforeEach
  void setUp() {
    exportCommand =
        new ExportCommand() {
          @Override
          protected StorageFactory createStorageFactory(String configFilePath) {
            return storageFactory;
          }
        };

    CommandLine cmd = new CommandLine(exportCommand);
    exportCommand.spec = cmd.getCommandSpec();
  }

  @AfterEach
  public void cleanup() throws IOException {
    cleanUpTempDir();
  }

  @Test
  void call_WithValidOutputDirectory_ShouldReturnZero() throws Exception {
    Path configFile = tempDir.resolve("config.properties");
    Files.createFile(configFile);

    Path outputDir = tempDir.resolve("output");
    Files.createDirectory(outputDir);

    exportCommand.outputFilePath = outputDir.toString();

    assertEquals(0, exportCommand.call());
  }

  @Test
  void call_WithInvalidOutputDirectory_ShouldThrowInvalidFileExtensionException()
      throws IOException {
    Path configFile = tempDir.resolve("config.properties");
    Files.createFile(configFile);

    Path outputDir = tempDir.resolve("output");
    outputDir.toFile().setWritable(false);

    exportCommand.outputFilePath = outputDir.toString();

    assertThrows(InvalidFileExtensionException.class, () -> exportCommand.call());
  }

  @Test
  void call_WithValidOutputFile_ShouldReturnZero() throws Exception {
    Path configFile = tempDir.resolve("config.properties");
    Files.createFile(configFile);

    Path outputFile = tempDir.resolve("output.csv");

    exportCommand.outputFilePath = outputFile.toString();

    assertEquals(0, exportCommand.call());
  }

  @Test
  void call_WithValidOutputFileInCurrentDirectory_ShouldReturnZero() throws Exception {
    Path configFile = tempDir.resolve("config.properties");
    Files.createFile(configFile);

    Path outputFile = Paths.get("output.csv");

    exportCommand.outputFilePath = outputFile.toString();

    assertEquals(0, exportCommand.call());
  }

  @Test
  void call_WithValidOutputFileWithoutDirectory_ShouldReturnZero() throws Exception {
    Path configFile = tempDir.resolve("config.properties");
    Files.createFile(configFile);

    exportCommand.outputFilePath = "output.csv";

    assertEquals(0, exportCommand.call());
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
      LOGGER.error("Failed to delete file: {}", file, e);
    }
  }
}
