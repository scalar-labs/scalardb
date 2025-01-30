package com.scalar.db.dataloader.cli.command.dataexport;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.service.StorageFactory;
import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Paths;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class ExportCommandTest {

  @AfterEach
  void removeFileIfCreated() {
    // To remove generated file if it is present
    String filePath = Paths.get("").toAbsolutePath() + "/sample.json";
    File file = new File(filePath);
    if (file.exists()) {
      file.deleteOnExit();
    }
  }

  @Test
  void call_withBlankScalarDBConfigurationFile_shouldThrowException() {
    ExportCommand exportCommand = new ExportCommand();
    exportCommand.configFilePath = "";
    exportCommand.dataChunkSize = 100;
    exportCommand.namespace = "scalar";
    exportCommand.table = "asset";
    exportCommand.outputDirectory = "";
    exportCommand.outputFileName = "sample.json";
    exportCommand.outputFormat = FileFormat.JSON;
    FileNotFoundException thrown =
        assertThrows(
            FileNotFoundException.class,
            exportCommand::call,
            "Expected to throw FileNotFound exception as configuration path is invalid");
    Assertions.assertEquals(" (No such file or directory)", thrown.getMessage());
  }

  @Test
  void call_withInvalidScalarDBConfigurationFile_shouldReturnOne() throws Exception {
    ExportCommand exportCommand = new ExportCommand();
    exportCommand.configFilePath = "scalardb.properties";
    exportCommand.dataChunkSize = 100;
    exportCommand.namespace = "scalar";
    exportCommand.table = "asset";
    exportCommand.outputDirectory = "";
    exportCommand.outputFileName = "sample.json";
    exportCommand.outputFormat = FileFormat.JSON;
    Assertions.assertEquals(1, exportCommand.call());
  }

  @Test
  void call_withMockedScalarDBConfiguration_shouldReturnNullPointerException() throws Exception {

    try (MockedStatic<StorageFactory> storageFactoryMockedStatic =
        Mockito.mockStatic(StorageFactory.class)) {
      StorageFactory storageFactory = Mockito.mock(StorageFactory.class);
      DistributedStorageAdmin mockAdmin = Mockito.mock(DistributedStorageAdmin.class);
      TableMetadata mockMetadata = Mockito.mock(TableMetadata.class);
      storageFactoryMockedStatic
          .when(() -> StorageFactory.create("src/test/resources/scalardb.properties"))
          .thenReturn(storageFactory);
      Mockito.when(storageFactory.getStorageAdmin()).thenReturn(mockAdmin);
      Mockito.when(mockAdmin.getTableMetadata("scalar", "asset")).thenReturn(mockMetadata);
      ExportCommand exportCommand = new ExportCommand();
      exportCommand.configFilePath = "src/test/resources/scalardb.properties";
      exportCommand.dataChunkSize = 100;
      exportCommand.namespace = "scalar";
      exportCommand.table = "asset";
      exportCommand.outputDirectory = "";
      exportCommand.outputFileName = "sample.json";
      exportCommand.outputFormat = FileFormat.JSON;
      NullPointerException thrown =
          assertThrows(
              NullPointerException.class,
              exportCommand::call,
              "Expected to throw FileNotFound exception as configuration path is invalid");
      Assertions.assertEquals(
          "Cannot invoke \"com.scalar.db.api.DistributedStorage.scan(com.scalar.db.api.Scan)\" because \"storage\" is null",
          thrown.getMessage());
    }
  }
}
