package com.scalar.db.dataloader.core.dataimport;

import com.scalar.db.dataloader.core.dataimport.datachunk.ImportDataChunkStatus;
import com.scalar.db.dataloader.core.dataimport.task.result.ImportTaskResult;
import com.scalar.db.dataloader.core.dataimport.transactionbatch.ImportTransactionBatchResult;
import com.scalar.db.dataloader.core.dataimport.transactionbatch.ImportTransactionBatchStatus;

public interface ImportEventListener {

  void onDataChunkStarted(ImportDataChunkStatus status);

  void addOrUpdateDataChunkStatus(ImportDataChunkStatus status);

  void onDataChunkCompleted(ImportDataChunkStatus status);

  void onAllDataChunksCompleted();

  void onTransactionBatchStarted(ImportTransactionBatchStatus batchStatus);

  void onTransactionBatchCompleted(ImportTransactionBatchResult batchResult);

  void onTaskComplete(ImportTaskResult taskResult);
}
