package com.scalar.db.dataloader.core.dataimport.datachunk;

/** Status of the import data chunk during the import process. */
public enum ImportDataChunkStatusState {
  /** Indicates that the import of the data chunk has started but has not yet progressed. */
  START,

  /** Indicates that the import of the data chunk is currently in progress. */
  IN_PROGRESS,

  /** Indicates that the import of the data chunk has been successfully completed. */
  COMPLETE
}
