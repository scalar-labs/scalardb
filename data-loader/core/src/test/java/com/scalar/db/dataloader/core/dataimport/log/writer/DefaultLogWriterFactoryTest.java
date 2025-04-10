package com.scalar.db.dataloader.core.dataimport.log.writer;

import com.scalar.db.dataloader.core.dataimport.log.ImportLoggerConfig;
import com.scalar.db.dataloader.core.dataimport.log.LogStorageLocation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

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
            LogWriterFactoryConfig.builder()
                .logStorageLocation(LogStorageLocation.LOCAL_FILE_STORAGE)
                .build(),
            ImportLoggerConfig.builder()
                .prettyPrint(false)
                .logSuccessRecords(false)
                .logRawSourceRecords(false)
                .logDirectoryPath("path")
                .build());
    LogWriter logWriter = defaultLogWriterFactory.createLogWriter(filePath);
    Assertions.assertEquals(LocalFileLogWriter.class, logWriter.getClass());
    logWriter.close();
  }

  @Test
  void createLogWriter_withValidFilePath_shouldReturnLogWriterObject() throws IOException {
    defaultLogWriterFactory =
        new DefaultLogWriterFactory(
            LogWriterFactoryConfig.builder()
                .logStorageLocation(LogStorageLocation.AWS_S3)
                .bucketName("bucket")
                .objectKey("ObjectKay")
                .s3AsyncClient(Mockito.mock(S3AsyncClient.class))
                .build(),
            ImportLoggerConfig.builder()
                .prettyPrint(false)
                .logSuccessRecords(false)
                .logRawSourceRecords(false)
                .logDirectoryPath("path")
                .build());
    LogWriter logWriter = defaultLogWriterFactory.createLogWriter(filePath);
    Assertions.assertEquals(AwsS3LogWriter.class, logWriter.getClass());
    logWriter.close();
  }
}
