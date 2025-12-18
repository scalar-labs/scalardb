package com.scalar.db.dataloader.core.dataimport;

import com.scalar.db.dataloader.core.dataimport.task.result.ImportTaskResult;
import com.scalar.db.dataloader.core.dataimport.transactionbatch.ImportTransactionBatchResult;
import com.scalar.db.dataloader.core.dataimport.transactionbatch.ImportTransactionBatchStatus;

/**
 * Listener interface for monitoring import events during the data loading process. Implementations
 * can use this to track progress and handle various stages of the import process.
 */
public interface ImportEventListener {

  /**
   * Called when the import process begins.
   *
   * @param status the current status of the import being processed
   */
  void onImportStarted(ImportStatus status);

  /**
   * Called when the import process is completed.
   *
   * @param status the final status of the completed import
   */
  void onImportCompleted(ImportStatus status);

  /**
   * Called when all imports have been processed. This indicates that the entire import process is
   * complete.
   */
  void onAllImportsCompleted();

  /**
   * Called when processing of a transaction batch begins. A transaction batch is a group of records
   * processed within a single transaction.
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
   * Called when an import task is completed. A task represents processing a single record.
   *
   * @param taskResult the result of the completed import task
   */
  void onTaskComplete(ImportTaskResult taskResult);
}
