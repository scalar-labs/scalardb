package com.scalar.db.dataloader.core.dataimport.task.result;

/**
 * Import target result status. Represents the outcome of processing a single import target (e.g., a
 * data row or record).
 */
public enum ImportTargetResultStatus {
  /**
   * The import failed due to validation errors (e.g., missing required fields, invalid formats).
   */
  VALIDATION_FAILED,

  /**
   * The import failed because the existing data could not be retrieved (e.g., due to I/O or
   * database issues).
   */
  RETRIEVAL_FAILED,

  /** The import failed due to missing required columns in the input data. */
  MISSING_COLUMNS,

  /**
   * The import was skipped because the data already exists and cannot be overwritten (in INSERT
   * mode).
   */
  DATA_ALREADY_EXISTS,

  /** The import failed because the required existing data was not found (e.g., in UPDATE mode). */
  DATA_NOT_FOUND,

  /** The import failed during the save operation (e.g., due to database write errors). */
  SAVE_FAILED,

  /** The import target was successfully saved to the database. */
  SAVED,

  /**
   * The import process was aborted before completion (e.g., due to a batch failure or external
   * cancellation).
   */
  ABORTED
}
