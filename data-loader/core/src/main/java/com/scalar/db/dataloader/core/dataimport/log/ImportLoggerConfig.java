package com.scalar.db.dataloader.core.dataimport.log;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class ImportLoggerConfig {
  String logDirectoryPath;
  boolean logSuccessRecords;
  boolean logRawSourceRecords;
  boolean prettyPrint;
}
