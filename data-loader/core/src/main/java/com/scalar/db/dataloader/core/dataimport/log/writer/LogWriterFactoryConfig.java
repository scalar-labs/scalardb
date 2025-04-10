package com.scalar.db.dataloader.core.dataimport.log.writer;

import com.scalar.db.dataloader.core.dataimport.log.LogStorageLocation;
import lombok.Builder;
import lombok.Value;
import software.amazon.awssdk.services.s3.S3AsyncClient;

@Builder
@Value
public class LogWriterFactoryConfig {
  LogStorageLocation logStorageLocation;
  S3AsyncClient s3AsyncClient;
  String bucketName;
  String objectKey;
}
