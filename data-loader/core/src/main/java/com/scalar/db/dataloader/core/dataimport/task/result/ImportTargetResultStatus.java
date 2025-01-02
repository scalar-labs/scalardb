package com.scalar.db.dataloader.core.dataimport.task.result;

public enum ImportTargetResultStatus {
  VALIDATION_FAILED,
  RETRIEVAL_FAILED,
  MISSING_COLUMNS,
  DATA_ALREADY_EXISTS,
  DATA_NOT_FOUND,
  SAVE_FAILED,
  SAVED,
  ABORTED
}
