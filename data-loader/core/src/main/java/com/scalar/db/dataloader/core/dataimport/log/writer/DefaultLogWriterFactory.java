package com.scalar.db.dataloader.core.dataimport.log.writer;

import com.scalar.db.dataloader.core.dataimport.log.ImportLoggerConfig;
import java.io.IOException;
import lombok.AllArgsConstructor;

/** A factory class to create log writers. */
@AllArgsConstructor
public class DefaultLogWriterFactory implements LogWriterFactory {

  private final LogWriterFactoryConfig config;
  private final ImportLoggerConfig importLoggerConfig;

  /**
   * Creates a log writer based on the configuration.
   *
   * @param logFilePath the path of the log file
   * @return the log writer
   */
  @Override
  public LogWriter createLogWriter(String logFilePath) throws IOException {
    LogWriter logWriter = null;
    switch (config.getLogStorageLocation()) {
      case LOCAL_FILE_STORAGE:
        logWriter = new LocalFileLogWriter(logFilePath, importLoggerConfig);
        break;
      case AWS_S3:
        logWriter =
            new AwsS3LogWriter(
                config.getS3AsyncClient(), config.getBucketName(), config.getObjectKey());
        break;
    }
    return logWriter;
  }
}
