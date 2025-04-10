package com.scalar.db.dataloader.core.dataimport.log.writer;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

public interface LogWriter extends AutoCloseable {

  void write(JsonNode sourceRecord) throws IOException;

  void flush() throws IOException;

  @Override
  void close() throws IOException;
}
