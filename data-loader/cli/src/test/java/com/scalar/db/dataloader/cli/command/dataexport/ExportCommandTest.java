package com.scalar.db.dataloader.cli.command.dataexport;

import static org.junit.jupiter.api.Assertions.*;

import com.scalar.db.dataloader.cli.exception.InvalidFileExtensionException;
import com.scalar.db.dataloader.cli.exception.InvalidFilePathException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

class ExportCommandTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExportCommandTest.class);
  @TempDir Path tempDir;

  private ExportCommand exportCommand;

  @BeforeEach
  void setUp() {
    exportCommand = new ExportCommand();
    CommandLine cmd = new CommandLine(exportCommand);
    exportCommand.spec = cmd.getCommandSpec();
  }

  @AfterEach
  public void cleanup() throws IOException {
    cleanUpTempDir();
  }

  @Test
  void call_WithValidConfigFileAndOutputDirectory_ShouldReturnZero() throws Exception {
    Path configFile = tempDir.resolve("config.properties");
    Files.createFile(configFile);

    Path outputDir = tempDir.resolve("output");
    Files.createDirectory(outputDir);

    exportCommand.configFilePath = configFile.toString();
    exportCommand.outputFilePath = outputDir.toString();

    assertEquals(0, exportCommand.call());
  }

  @Test
  void call_WithInvalidConfigFile_ShouldThrowInvalidFilePathException() {
    exportCommand.configFilePath = "nonexistent.properties";
    assertThrows(InvalidFilePathException.class, () -> exportCommand.call());
  }

  @Test
  void call_WithInvalidOutputDirectory_ShouldThrowInvalidFileExtensionException()
      throws IOException {
    Path configFile = tempDir.resolve("config.properties");
    Files.createFile(configFile);

    Path outputDir = tempDir.resolve("output");
    outputDir.toFile().setWritable(false);

    exportCommand.configFilePath = configFile.toString();
    exportCommand.outputFilePath = outputDir.toString();

    assertThrows(InvalidFileExtensionException.class, () -> exportCommand.call());
  }

  @Test
  void call_WithValidOutputFile_ShouldReturnZero() throws Exception {
    Path configFile = tempDir.resolve("config.properties");
    Files.createFile(configFile);

    Path outputFile = tempDir.resolve("output.csv");

    exportCommand.configFilePath = configFile.toString();
    exportCommand.outputFilePath = outputFile.toString();

    assertEquals(0, exportCommand.call());
  }

  @Test
  void call_WithValidOutputFileInCurrentDirectory_ShouldReturnZero() throws Exception {
    Path configFile = tempDir.resolve("config.properties");
    Files.createFile(configFile);

    Path outputFile = Paths.get("output.csv");

    exportCommand.configFilePath = configFile.toString();
    exportCommand.outputFilePath = outputFile.toString();

    assertEquals(0, exportCommand.call());
  }

  @Test
  void call_WithValidOutputFileWithoutDirectory_ShouldReturnZero() throws Exception {
    Path configFile = tempDir.resolve("config.properties");
    Files.createFile(configFile);

    exportCommand.configFilePath = configFile.toString();
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
