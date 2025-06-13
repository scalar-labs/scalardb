package com.scalar.db.dataloader.core.dataimport;

import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.dataloader.core.dataimport.controlfile.ControlFile;
import com.scalar.db.dataloader.core.dataimport.controlfile.ControlFileValidationLevel;
import com.scalar.db.dataloader.core.dataimport.log.LogMode;
import lombok.Builder;
import lombok.Data;

/** Import options to import data into one or more ScalarDB tables. */
@SuppressWarnings({"SameNameButDifferent", "MissingSummary"})
@Builder
@Data
public class ImportOptions {

  @Builder.Default private final ImportMode importMode = ImportMode.UPSERT;
  @Builder.Default private final boolean requireAllColumns = false;
  @Builder.Default private final FileFormat fileFormat = FileFormat.JSON;
  @Builder.Default private final boolean prettyPrint = false;
  @Builder.Default private final boolean ignoreNullValues = false;
  @Builder.Default private final LogMode logMode = LogMode.SPLIT_BY_DATA_CHUNK;

  @Builder.Default
  private final ControlFileValidationLevel controlFileValidationLevel =
      ControlFileValidationLevel.MAPPED;

  @Builder.Default private final char delimiter = ',';

  @Builder.Default private final boolean logSuccessRecords = false;
  @Builder.Default private final boolean logRawRecord = false;

  private final int dataChunkSize;
  private final int transactionBatchSize;
  private final ControlFile controlFile;
  private final String namespace;
  private final String tableName;
  private final int maxThreads;
  private final String customHeaderRow;
  private final int dataChunkQueueSize;
}
