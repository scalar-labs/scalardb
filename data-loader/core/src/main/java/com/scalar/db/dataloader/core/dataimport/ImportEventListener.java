package com.scalar.db.dataloader.core.dataimport;

import com.scalar.db.dataloader.core.dataimport.datachunk.ImportDataChunkStatus;
import com.scalar.db.dataloader.core.dataimport.task.result.ImportTaskResult;
import com.scalar.db.dataloader.core.dataimport.transactionbatch.ImportTransactionBatchResult;
import com.scalar.db.dataloader.core.dataimport.transactionbatch.ImportTransactionBatchStatus;

/**
 * Listener interface for monitoring import events during the data loading process. Implementations
 * can use this to track progress and handle various stages of the import process.
 */
public interface ImportEventListener {

  /**
   * Called when processing of a data chunk begins.
   *
   * @param status the current status of the data chunk being processed
   */
  void onDataChunkStarted(ImportDataChunkStatus status);

  /**
   * Updates or adds new status information for a data chunk.
   *
   * @param status the updated status information for the data chunk
   */
  void addOrUpdateDataChunkStatus(ImportDataChunkStatus status);

  /**
   * Called when processing of a data chunk is completed.
   *
   * @param status the final status of the completed data chunk
   */
  void onDataChunkCompleted(ImportDataChunkStatus status);

  /**
   * Called when all data chunks have been processed. This indicates that the entire chunked import
   * process is complete.
   */
  void onAllDataChunksCompleted();

  /**
   * Called when processing of a transaction batch begins.
   *
   * @param batchStatus the initial status of the transaction batch
   */
  void onTransactionBatchStarted(ImportTransactionBatchStatus batchStatus);

  /**
   * Called when processing of a transaction batch is completed.
   *
   * @param batchResult the result of the completed transaction batch
   */
  void onTransactionBatchCompleted(ImportTransactionBatchResult batchResult);

  /**
   * Called when an import task is completed.
   *
   * @param taskResult the result of the completed import task
   */
  void onTaskComplete(ImportTaskResult taskResult);
}
