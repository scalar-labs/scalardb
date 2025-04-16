package com.scalar.db.dataloader.core.dataimport.log.writer;

/**
 * Represents the different types of log files used in the data import process. Each type serves a
 * specific purpose in tracking the import operation's results.
 */
public enum LogFileType {
  /**
   * Represents a log file that records successful import operations. These logs contain records
   * that were successfully processed and imported.
   */
  SUCCESS,

  /**
   * Represents a log file that records failed import operations. These logs contain records that
   * failed to be processed or imported, along with information about the failure.
   */
  FAILURE,

  /**
   * Represents a log file that provides a summary of the import operation. These logs contain
   * aggregated statistics and overall results of the import process.
   */
  SUMMARY
}
