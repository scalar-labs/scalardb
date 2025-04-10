package com.scalar.db.dataloader.core.dataimport.log.writer;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;

@AllArgsConstructor
public class AwsS3LogWriter implements LogWriter {

  private final S3AsyncClient s3AsyncClient;
  private final String bucketName;
  private final String objectKey;

  @Override
  public void write(JsonNode sourceRecord) throws IOException {
    // Implementation to write content to cloud storage
  }

  @Override
  public void flush() throws IOException {
    // Implementation to flush content to cloud storage
  }

  @Override
  public void close() throws IOException {
    // Implementation to close the cloud storage connection
  }
}
