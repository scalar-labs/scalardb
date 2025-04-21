package com.scalar.db.dataloader.core.dataimport.log.writer;

import com.scalar.db.dataloader.core.dataimport.log.ImportLoggerConfig;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DefaultLogWriterFactoryTest {

  String filePath = Paths.get("").toAbsolutePath() + "/sample.log";
  DefaultLogWriterFactory defaultLogWriterFactory;

  @AfterEach
  void removeFileIfCreated() {
    File file = new File(filePath);
    if (file.exists()) {
      file.deleteOnExit();
    }
  }

  @Test
  void createLogWriter_withValidLocalLogFilePath_shouldReturnLocalFileLogWriterObject()
      throws IOException {
    defaultLogWriterFactory =
        new DefaultLogWriterFactory(
            ImportLoggerConfig.builder()
                .prettyPrint(false)
                .isLogSuccessRecords(false)
                .isLogRawSourceRecords(false)
                .logDirectoryPath("path")
                .build());
    LogWriter logWriter = defaultLogWriterFactory.createLogWriter(filePath);
    Assertions.assertEquals(LocalFileLogWriter.class, logWriter.getClass());
    logWriter.close();
  }
}
