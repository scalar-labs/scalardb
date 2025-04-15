package com.scalar.db.dataloader.core.dataimport.log.writer;

import com.scalar.db.dataloader.core.dataimport.log.ImportLoggerConfig;
import java.io.IOException;
import lombok.AllArgsConstructor;

/** A factory class to create log writers. */
@AllArgsConstructor
public class DefaultLogWriterFactory implements LogWriterFactory {

  private final ImportLoggerConfig importLoggerConfig;

  /**
   * Creates a log writer object
   *
   * @param logFilePath the path of the log file
   * @return the log writer
   */
  @Override
  public LogWriter createLogWriter(String logFilePath) throws IOException {
    return new LocalFileLogWriter(logFilePath, importLoggerConfig);
  }
}
