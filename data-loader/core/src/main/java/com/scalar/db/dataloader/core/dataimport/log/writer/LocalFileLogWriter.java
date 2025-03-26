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

public class LocalFileLogWriter implements LogWriter {
  private final JsonGenerator logWriter;
  private final DataLoaderObjectMapper objectMapper;

  /**
   * Creates an instance of LocalFileLogWriter with the specified file path and log file type.
   *
   * @param filePath the file path
   * @throws IOException if an I/O error occurs
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

  @Override
  public void write(JsonNode sourceRecord) throws IOException {
    if (sourceRecord == null) {
      return;
    }
    synchronized (logWriter) {
      objectMapper.writeValue(logWriter, sourceRecord);
    }
  }

  @Override
  public void flush() throws IOException {
    logWriter.flush();
  }

  @Override
  public void close() throws IOException {
    if (logWriter.isClosed()) {
      return;
    }
    logWriter.writeEndArray();
    logWriter.flush();
    logWriter.close();
  }
}
