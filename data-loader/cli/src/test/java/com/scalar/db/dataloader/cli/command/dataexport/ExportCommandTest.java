package com.scalar.db.dataloader.cli.command.dataexport;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.dataloader.core.ScalarDbMode;
import java.io.File;
import java.nio.file.Paths;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            exportCommand::call,
            "Expected to throw FileNotFound exception as configuration path is invalid");
    Assertions.assertEquals(
        CoreError.DATA_LOADER_CONFIG_FILE_PATH_BLANK.buildMessage(), thrown.getMessage());
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
  void call_withScalarDBModeTransaction_WithInvalidConfigurationFile_shouldReturnOne()
      throws Exception {
    ExportCommand exportCommand = new ExportCommand();
    exportCommand.configFilePath = "scalardb.properties";
    exportCommand.dataChunkSize = 100;
    exportCommand.namespace = "scalar";
    exportCommand.table = "asset";
    exportCommand.outputDirectory = "";
    exportCommand.outputFileName = "sample.json";
    exportCommand.outputFormat = FileFormat.JSON;
    exportCommand.scalarDbMode = ScalarDbMode.TRANSACTION;
    Assertions.assertEquals(1, exportCommand.call());
  }
}
