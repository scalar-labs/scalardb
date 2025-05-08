package com.scalar.db.dataloader.core.dataimport.log.writer;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;

/**
 * An interface for writing log entries to a destination. This interface extends {@link
 * AutoCloseable} to ensure proper resource cleanup. Implementations of this interface handle the
 * details of writing log entries to various destinations such as files, databases, or other storage
 * systems.
 */
public interface LogWriter extends AutoCloseable {

  /**
   * Writes a JSON record to the log.
   *
   * @param sourceRecord the JSON record to write
   * @throws IOException if an I/O error occurs while writing the record
   */
  void write(JsonNode sourceRecord) throws IOException;

  /**
   * Flushes any buffered data to the underlying storage.
   *
   * @throws IOException if an I/O error occurs while flushing
   */
  void flush() throws IOException;

  /**
   * Closes this log writer and releases any system resources associated with it. This method should
   * be called when the log writer is no longer needed to ensure proper cleanup of resources.
   *
   * @throws IOException if an I/O error occurs while closing the log writer
   */
  @Override
  void close() throws IOException;
}
