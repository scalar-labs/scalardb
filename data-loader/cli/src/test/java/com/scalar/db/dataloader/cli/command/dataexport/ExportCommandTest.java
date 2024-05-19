package com.scalar.db.dataloader.cli.command.dataexport;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.cli.exception.InvalidFileExtensionException;
import com.scalar.db.dataloader.core.tablemetadata.TableMetadataException;
import com.scalar.db.dataloader.core.tablemetadata.TableMetadataService;
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
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

class ExportCommandTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExportCommandTest.class);
  @TempDir Path tempDir;
  @Mock StorageFactory storageFactory;
  @Mock TableMetadataService tableMetadataService;
  @Mock DistributedStorageAdmin storageAdmin;
  @Mock TableMetadata tableMetadata;

  @InjectMocks private ExportCommand exportCommand;

  private AutoCloseable closeable;

  @BeforeEach
  void setUp() throws TableMetadataException {
    closeable = MockitoAnnotations.openMocks(this);
    exportCommand =
        new ExportCommand() {
          @Override
          protected StorageFactory createStorageFactory(String configFilePath) {
            return storageFactory;
          }

          @Override
          protected TableMetadataService createTableMetadataService(StorageFactory storageFactory) {
            return tableMetadataService;
          }
        };

    CommandLine cmd = new CommandLine(exportCommand);
    exportCommand.spec = cmd.getCommandSpec();
    doReturn(storageAdmin).when(storageFactory).getStorageAdmin();
    doReturn(tableMetadata).when(tableMetadataService).getTableMetadata(anyString(), anyString());
  }

  @AfterEach
  public void cleanup() throws Exception {
    cleanUpTempDir();
    closeable.close();
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
    exportCommand.configFilePath = ExportCommandOptions.DEFAULT_CONFIG_FILE_NAME;

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
