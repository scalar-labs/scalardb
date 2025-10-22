package com.scalar.db.dataloader.core.dataimport.log.writer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.db.dataloader.core.DataLoaderObjectMapper;
import com.scalar.db.dataloader.core.dataimport.log.ImportLoggerConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import javax.annotation.Nullable;

/**
 * An implementation of {@link LogWriter} that writes log entries to a local file. This class writes
 * JSON records to a file as a JSON array, with each record being an element in the array. It
 * handles file creation, appending, and proper JSON formatting.
 */
public class LocalFileLogWriter implements LogWriter {
  private final JsonGenerator logWriter;
  private final DataLoaderObjectMapper objectMapper;
  private boolean isEndArrayWritten = false;

  /**
   * Creates an instance of LocalFileLogWriter with the specified file path and configuration.
   *
   * @param filePath the path where the log file will be created or appended to
   * @param importLoggerConfig the configuration for the logger, including formatting options
   * @throws IOException if an I/O error occurs while creating or opening the file
   */
  public LocalFileLogWriter(String filePath, ImportLoggerConfig importLoggerConfig)
      throws IOException {
    Path path = Paths.get(filePath);
    this.objectMapper = new DataLoaderObjectMapper();
    this.logWriter =
        objectMapper
            .getFactory()
            .createGenerator(
                Files.newBufferedWriter(
                    path, StandardOpenOption.CREATE, StandardOpenOption.APPEND));
    // Start the JSON array
    if (importLoggerConfig.isPrettyPrint()) this.logWriter.useDefaultPrettyPrinter();
    this.logWriter.writeStartArray();
    this.logWriter.flush();
  }

  /**
   * Writes a JSON record to the log file. If the source record is null, this method does nothing.
   * The method is synchronized to ensure thread safety when writing to the file.
   *
   * @param sourceRecord the JSON record to write
   * @throws IOException if an I/O error occurs while writing the record
   */
  @Override
  public synchronized void write(@Nullable JsonNode sourceRecord) throws IOException {
    if (sourceRecord == null) {
      return;
    }
    objectMapper.writeValue(logWriter, sourceRecord);
  }

  /**
   * Flushes any buffered data to the log file.
   *
   * @throws IOException if an I/O error occurs while flushing
   */
  @Override
  public synchronized void flush() throws IOException {
    logWriter.flush();
  }

  /**
   * Closes the log writer, properly ending the JSON array and releasing resources. If the writer is
   * already closed, this method does nothing.
   *
   * @throws IOException if an I/O error occurs while closing the writer
   */
  @Override
  public synchronized void close() throws IOException {
    if (logWriter.isClosed()) {
      return;
    }
    if (!isEndArrayWritten) {
      logWriter.writeEndArray();
      isEndArrayWritten = true;
    }
    logWriter.flush();
    logWriter.close();
  }
}
