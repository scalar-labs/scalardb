package com.scalar.db.dataloader.core.dataimport;

/** Status of the import process. */
public enum ImportStatusState {
  /** Indicates that the import has started but has not yet progressed. */
  START,

  /** Indicates that the import is currently in progress. */
  IN_PROGRESS,

  /** Indicates that the import has been successfully completed. */
  COMPLETE
}
