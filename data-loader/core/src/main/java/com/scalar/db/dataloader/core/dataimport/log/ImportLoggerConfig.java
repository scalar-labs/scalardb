package com.scalar.db.dataloader.core.dataimport.log;

import lombok.Builder;
import lombok.Value;

/**
 * Configuration class for import loggers. This class uses Lombok's {@code @Value} annotation to
 * create an immutable class and {@code @Builder} annotation to provide a builder pattern for
 * creating instances.
 */
@Value
@Builder
@SuppressWarnings("SameNameButDifferent")
public class ImportLoggerConfig {
  /**
   * The directory path where log files will be stored. This path should end with a directory
   * separator (e.g., "/").
   */
  String logDirectoryPath;

  /**
   * Whether to log records that were successfully imported. If true, successful import operations
   * will be logged to success log files.
   */
  boolean isLogSuccessRecordsEnabled;

  /**
   * Whether to log raw source records that failed to be imported. If true, failed import operations
   * will be logged to failure log files.
   */
  boolean isLogRawSourceRecordsEnabled;

  /**
   * Whether to format the logs with pretty printing. If true, the JSON logs will be formatted with
   * indentation for better readability.
   */
  boolean prettyPrint;
}
