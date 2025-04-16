package com.scalar.db.dataloader.core.dataimport.log.writer;

import java.io.IOException;

/**
 * A factory interface for creating {@link LogWriter} instances. This interface abstracts the
 * creation of log writers, allowing different implementations to create different types of log
 * writers based on the application's needs.
 */
public interface LogWriterFactory {

  /**
   * Creates a new log writer for the specified log file path.
   *
   * @param logFilePath the path where the log file will be created or appended to
   * @return a new {@link LogWriter} instance
   * @throws IOException if an I/O error occurs while creating the log writer
   */
  LogWriter createLogWriter(String logFilePath) throws IOException;
}
