package com.scalar.db.dataloader.core.dataimport.log.writer;

import com.scalar.db.dataloader.core.dataimport.log.ImportLoggerConfig;
import java.io.IOException;
import lombok.AllArgsConstructor;

/**
 * The default implementation of {@link LogWriterFactory} that creates {@link LocalFileLogWriter}
 * instances. This factory uses the provided {@link ImportLoggerConfig} to configure the log writers
 * it creates. It's annotated with Lombok's {@code @AllArgsConstructor} to automatically generate a
 * constructor that initializes the configuration field.
 */
@AllArgsConstructor
public class DefaultLogWriterFactory implements LogWriterFactory {

  private final ImportLoggerConfig importLoggerConfig;

  /**
   * Creates a {@link LocalFileLogWriter} for the specified log file path. The created log writer
   * will be configured using the {@link ImportLoggerConfig} that was provided to this factory
   * during construction.
   *
   * @param logFilePath the path where the log file will be created or appended to
   * @return a new {@link LogWriter} instance that writes to the specified file
   * @throws IOException if an I/O error occurs while creating the log writer
   */
  @Override
  public LogWriter createLogWriter(String logFilePath) throws IOException {
    return new LocalFileLogWriter(logFilePath, importLoggerConfig);
  }
}
