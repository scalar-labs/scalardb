package com.scalar.db.dataloader.cli.command.dataimport;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.dataloader.core.dataimport.ImportMode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

class ImportCommandTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ImportCommandTest.class);
  @TempDir Path tempDir;

  private ImportCommand importCommand;

  @BeforeEach
  void setUp() {
    importCommand = new ImportCommand();
    CommandLine cmd = new CommandLine(importCommand);
    importCommand.spec = cmd.getCommandSpec();
  }

  @AfterEach
  void cleanup() throws IOException {
    cleanUpTempDir();
  }

  @Test
  void call_WithoutValidConfigFile_ShouldThrowException() throws Exception {
    Path configFile = tempDir.resolve("config.properties");
    Files.createFile(configFile);
    Path importFile = tempDir.resolve("import.json");
    Files.createFile(importFile);
    importCommand.configFilePath = configFile.toString();
    importCommand.namespace = "sample";
    importCommand.tableName = "table";
    importCommand.sourceFileFormat = FileFormat.JSON;
    importCommand.sourceFilePath = importFile.toString();
    importCommand.importMode = ImportMode.UPSERT;
    assertThrows(IllegalArgumentException.class, () -> importCommand.call());
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
