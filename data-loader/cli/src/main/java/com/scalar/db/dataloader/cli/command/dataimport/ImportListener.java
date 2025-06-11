package com.scalar.db.dataloader.cli.command.dataimport;

import com.scalar.db.dataloader.core.dataimport.ImportEventListener;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportDataChunkStatus;
import com.scalar.db.dataloader.core.dataimport.task.result.ImportTaskResult;
import com.scalar.db.dataloader.core.dataimport.transactionbatch.ImportTransactionBatchResult;
import com.scalar.db.dataloader.core.dataimport.transactionbatch.ImportTransactionBatchStatus;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImportListener implements ImportEventListener {

  private static final Logger logger = LoggerFactory.getLogger(ImportListener.class);
  private final AtomicInteger totalSuccessImportCount = new AtomicInteger(0);
  private final AtomicInteger totalFailureImportCount = new AtomicInteger(0);
  private final ConcurrentHashMap<Integer, AtomicInteger> dataChunkImportCountMap =
      new ConcurrentHashMap<>();
  private static final String DATA_CHUNK_COMPLETED_SUCCESS_MSG =
      "\u2713 Chunk %d: %d records imported (%ds) successfully";
  private static final String DATA_CHUNK_COMPLETED_FAILURE_MSG =
      "\u274C Chunk %d: %d records failed to import";
  private static final String DATA_CHUNK_IN_PROCESS_MSG =
      "\uD83D\uDD04 Chunk %d: Processing... %d records so far imported";

  /**
   * Called when processing of a data chunk begins.
   *
   * @param status the current status of the data chunk being processed
   */
  @Override
  public void onDataChunkStarted(ImportDataChunkStatus status) {}

  /**
   * Called when processing of a data chunk is completed.
   *
   * @param status the final status of the completed data chunk
   */
  @Override
  public void onDataChunkCompleted(ImportDataChunkStatus status) {
    if (status.getSuccessCount() > 0) {
      logger.info(
          String.format(
              DATA_CHUNK_COMPLETED_SUCCESS_MSG,
              status.getDataChunkId(),
              status.getSuccessCount(),
              status.getTotalDurationInMilliSeconds() / 1000));
      this.totalSuccessImportCount.addAndGet(status.getSuccessCount());
    }
    if (status.getFailureCount() > 0) {
      logger.info(
          String.format(
              DATA_CHUNK_COMPLETED_FAILURE_MSG, status.getDataChunkId(), status.getFailureCount()));
      this.totalFailureImportCount.addAndGet(status.getFailureCount());
    }
  }

  /**
   * Called when all data chunks have been processed. This indicates that the entire chunked import
   * process is complete.
   */
  @Override
  public void onAllDataChunksCompleted() {
    System.out.println("Logger Factory: " + LoggerFactory.getILoggerFactory().getClass());
    if (totalSuccessImportCount.get() > 0) {}
  }

  /**
   * Called when processing of a transaction batch begins.
   *
   * @param batchStatus the initial status of the transaction batch
   */
  @Override
  public void onTransactionBatchStarted(ImportTransactionBatchStatus batchStatus) {}

  /**
   * Called when processing of a transaction batch is completed.
   *
   * @param batchResult the result of the completed transaction batch
   */
  @Override
  public void onTransactionBatchCompleted(ImportTransactionBatchResult batchResult) {
    if (batchResult.isSuccess()) {
      Integer dataChunkId = batchResult.getDataChunkId();
      dataChunkImportCountMap.compute(
          dataChunkId,
          (k, v) -> {
            if (v == null) {
              return new AtomicInteger(batchResult.getRecords().size());
            } else {
              v.addAndGet(batchResult.getRecords().size());
              return v;
            }
          });
      logger.info(
          String.format(
              DATA_CHUNK_IN_PROCESS_MSG,
              dataChunkId,
              dataChunkImportCountMap.get(dataChunkId).get()));
    }
  }

  /**
   * Called when an import task is completed.
   *
   * @param taskResult the result of the completed import task
   */
  @Override
  public void onTaskComplete(ImportTaskResult taskResult) {}
}
