package com.scalar.db.dataloader.core.dataimport.task.result;

/** Enum to indicate the import result status */
public enum ImportResultStatus {
  SUCCESS,
  PARTIAL_SUCCESS,
  FAILURE,
  VALIDATION_FAILED,
  RETRIEVAL_FAILED,
  MAPPING_FAILED,
  TIMEOUT,
  CANCELLED
}
