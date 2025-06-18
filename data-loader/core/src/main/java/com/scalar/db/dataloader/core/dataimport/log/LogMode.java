package com.scalar.db.dataloader.core.dataimport.log;

/**
 * Log modes available for import logging. Determines how logs are organized and written during the
 * import process.
 */
public enum LogMode {
  /**
   * Logs all import-related messages into a single log file. Useful for centralized and sequential
   * log analysis.
   */
  SINGLE_FILE,

  /**
   * Splits logs into separate files for each data chunk being imported. Useful for parallel
   * processing and debugging individual chunks independently.
   */
  SPLIT_BY_DATA_CHUNK
}
