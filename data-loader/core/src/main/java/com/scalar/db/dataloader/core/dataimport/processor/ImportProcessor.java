package com.scalar.db.dataloader.core.dataimport.processor;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.dataloader.core.ScalarDBMode;
import com.scalar.db.dataloader.core.dataimport.ImportEventListener;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportDataChunk;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportDataChunkStatus;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportDataChunkStatusState;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportRow;
import com.scalar.db.dataloader.core.dataimport.task.ImportStorageTask;
import com.scalar.db.dataloader.core.dataimport.task.ImportTaskParams;
import com.scalar.db.dataloader.core.dataimport.task.ImportTransactionalTask;
import com.scalar.db.dataloader.core.dataimport.task.result.ImportTargetResultStatus;
import com.scalar.db.dataloader.core.dataimport.task.result.ImportTaskResult;
import com.scalar.db.dataloader.core.dataimport.transactionbatch.ImportTransactionBatch;
import com.scalar.db.dataloader.core.dataimport.transactionbatch.ImportTransactionBatchResult;
import com.scalar.db.dataloader.core.dataimport.transactionbatch.ImportTransactionBatchStatus;
import com.scalar.db.dataloader.core.util.ConfigUtil;
import com.scalar.db.exception.transaction.TransactionException;
import java.io.BufferedReader;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract class that handles the processing of data imports into ScalarDB. This processor
 * supports both transactional and non-transactional (storage) modes and provides event notification
 * capabilities for monitoring the import process.
 */
@RequiredArgsConstructor
public abstract class ImportProcessor {

  final ImportProcessorParams params;
  private static final Logger LOGGER = LoggerFactory.getLogger(ImportProcessor.class);
  private final List<ImportEventListener> listeners = new ArrayList<>();

  /**
   * Processes the source data from the given import file.
   *
   * <p>This method reads data from the provided {@link BufferedReader}, processes it in chunks, and
   * batches transactions according to the specified sizes. The processing can be done in either
   * transactional or storage mode, depending on the configured {@link ScalarDBMode}.
   *
   * @param dataChunkSize the number of records to include in each data chunk for parallel
   *     processing
   * @param transactionBatchSize the number of records to group together in a single transaction
   *     (only used in transaction mode)
   * @param reader the {@link BufferedReader} used to read the source file
   * @return a map of {@link ImportDataChunkStatus} objects indicating the processing status and
   *     results of each data chunk
   */
  public abstract ConcurrentHashMap<Integer, ImportDataChunkStatus> process(
      int dataChunkSize, int transactionBatchSize, BufferedReader reader);

  /**
   * Add import event listener to listener list
   *
   * @param listener import event listener
   */
  public void addListener(ImportEventListener listener) {
    listeners.add(listener);
  }

  /**
   * Remove import event listener from listener list
   *
   * @param listener import event listener
   */
  public void removeListener(ImportEventListener listener) {
    listeners.remove(listener);
  }

  /**
   * Notify once the task is completed
   *
   * @param result task result object
   */
  protected void notifyStorageRecordCompleted(ImportTaskResult result) {
    // Add data to summary, success logs with/without raw data
    for (ImportEventListener listener : listeners) {
      listener.onTaskComplete(result);
    }
  }

  /**
   * Notify once the data chunk process is started
   *
   * @param status data chunk status object
   */
  protected void notifyDataChunkStarted(ImportDataChunkStatus status) {
    for (ImportEventListener listener : listeners) {
      listener.onDataChunkStarted(status);
      listener.addOrUpdateDataChunkStatus(status);
    }
  }

  /**
   * Notify once the data chunk process is completed
   *
   * @param status data chunk status object
   */
  protected void notifyDataChunkCompleted(ImportDataChunkStatus status) {
    for (ImportEventListener listener : listeners) {
      listener.onDataChunkCompleted(status);
      listener.addOrUpdateDataChunkStatus(status);
    }
  }

  /**
   * Notify once the import transaction batch is started
   *
   * @param batchStatus import transaction batch status object
   */
  protected void notifyTransactionBatchStarted(ImportTransactionBatchStatus batchStatus) {
    for (ImportEventListener listener : listeners) {
      listener.onTransactionBatchStarted(batchStatus);
    }
  }

  /**
   * Notify once the import transaction batch is completed
   *
   * @param batchResult import transaction batch result object
   */
  protected void notifyTransactionBatchCompleted(ImportTransactionBatchResult batchResult) {
    for (ImportEventListener listener : listeners) {
      listener.onTransactionBatchCompleted(batchResult);
    }
  }

  /** Notify when all data chunks processes are completed */
  protected void notifyAllDataChunksCompleted() {
    for (ImportEventListener listener : listeners) {
      listener.onAllDataChunksCompleted();
    }
  }

  /**
   * Splits a data chunk into smaller transaction batches for processing. This method is used in
   * transaction mode to group records together for atomic processing.
   *
   * @param dataChunk the data chunk to split into batches
   * @param batchSize the maximum number of records per transaction batch
   * @return a list of {@link ImportTransactionBatch} objects representing the split batches
   */
  private List<ImportTransactionBatch> splitIntoTransactionBatches(
      ImportDataChunk dataChunk, int batchSize) {
    List<ImportTransactionBatch> transactionBatches = new ArrayList<>();
    AtomicInteger transactionBatchIdCounter = new AtomicInteger(0);

    List<ImportRow> importRows = dataChunk.getSourceData();
    for (int i = 0; i < importRows.size(); i += batchSize) {
      int endIndex = Math.min(i + batchSize, importRows.size());
      List<ImportRow> transactionBatchData = importRows.subList(i, endIndex);
      int transactionBatchId = transactionBatchIdCounter.getAndIncrement();
      ImportTransactionBatch transactionBatch =
          ImportTransactionBatch.builder()
              .transactionBatchId(transactionBatchId)
              .sourceData(transactionBatchData)
              .build();
      transactionBatches.add(transactionBatch);
    }
    return transactionBatches;
  }

  /**
   * Processes a single transaction batch within a data chunk. Creates a new transaction, processes
   * all records in the batch, and commits or aborts the transaction based on the success of all
   * operations.
   *
   * @param dataChunk the parent data chunk containing this batch
   * @param transactionBatch the batch of records to process in a single transaction
   * @return an {@link ImportTransactionBatchResult} containing the processing results and any
   *     errors
   */
  private ImportTransactionBatchResult processTransactionBatch(
      ImportDataChunk dataChunk, ImportTransactionBatch transactionBatch) {
    ImportTransactionBatchStatus status =
        ImportTransactionBatchStatus.builder()
            .dataChunkId(dataChunk.getDataChunkId())
            .transactionBatchId(transactionBatch.getTransactionBatchId())
            .build();
    notifyTransactionBatchStarted(status);
    List<ImportTaskResult> importRecordResult = new ArrayList<>();
    boolean isSuccess;
    String error = "";
    DistributedTransaction transaction = null;
    try {
      // Create the ScalarDB transaction
      transaction = params.getDistributedTransactionManager().start();

      // Loop over the transaction batch and process each record
      for (ImportRow importRow : transactionBatch.getSourceData()) {
        ImportTaskParams taskParams =
            ImportTaskParams.builder()
                .sourceRecord(importRow.getSourceData())
                .dataChunkId(dataChunk.getDataChunkId())
                .rowNumber(importRow.getRowNumber())
                .importOptions(params.getImportOptions())
                .tableColumnDataTypes(params.getTableColumnDataTypes())
                .tableMetadataByTableName(params.getTableMetadataByTableName())
                .dao(params.getDao())
                .build();
        importRecordResult.add(new ImportTransactionalTask(taskParams, transaction).execute());
      }
      isSuccess =
          importRecordResult.stream()
              .allMatch(
                  importTaskResult ->
                      importTaskResult.getTargets().stream()
                          .allMatch(
                              targetResult ->
                                  targetResult.getStatus().equals(ImportTargetResultStatus.SAVED)));

      // Check and  Commit the transaction
      if (isSuccess) {
        transaction.commit();
      } else {
        transaction.abort();
        error = "All transactions are aborted";
      }

    } catch (TransactionException e) {
      isSuccess = false;
      LOGGER.error(e.getMessage());
      try {
        if (transaction != null) {
          transaction.abort(); // Ensure transaction is aborted
        }
      } catch (TransactionException abortException) {
        LOGGER.error(
            "Failed to abort transaction: {}", abortException.getMessage(), abortException);
      }
      error = e.getMessage();
    }
    ImportTransactionBatchResult importTransactionBatchResult =
        ImportTransactionBatchResult.builder()
            .transactionBatchId(transactionBatch.getTransactionBatchId())
            .success(isSuccess)
            .dataChunkId(dataChunk.getDataChunkId())
            .records(importRecordResult)
            .errors(Collections.singletonList(error))
            .build();
    notifyTransactionBatchCompleted(importTransactionBatchResult);
    return importTransactionBatchResult;
  }

  /**
   * Processes a single record in storage mode (non-transactional). Each record is processed
   * independently without transaction guarantees.
   *
   * @param dataChunk the parent data chunk containing this record
   * @param importRow the record to process
   * @return an {@link ImportTaskResult} containing the processing result for the record
   */
  private ImportTaskResult processStorageRecord(ImportDataChunk dataChunk, ImportRow importRow) {
    ImportTaskParams taskParams =
        ImportTaskParams.builder()
            .sourceRecord(importRow.getSourceData())
            .dataChunkId(dataChunk.getDataChunkId())
            .rowNumber(importRow.getRowNumber())
            .importOptions(params.getImportOptions())
            .tableColumnDataTypes(params.getTableColumnDataTypes())
            .tableMetadataByTableName(params.getTableMetadataByTableName())
            .dao(params.getDao())
            .build();
    ImportTaskResult importRecordResult =
        new ImportStorageTask(taskParams, params.getDistributedStorage()).execute();

    ImportTaskResult modifiedTaskResult =
        ImportTaskResult.builder()
            .rowNumber(importRecordResult.getRowNumber())
            .rawRecord(importRecordResult.getRawRecord())
            .targets(importRecordResult.getTargets())
            .dataChunkId(dataChunk.getDataChunkId())
            .build();
    notifyStorageRecordCompleted(modifiedTaskResult);
    return modifiedTaskResult;
  }

  /**
   * Processes a complete data chunk using parallel execution. The processing mode (transactional or
   * storage) is determined by the configured {@link ScalarDBMode}.
   *
   * @param dataChunk the data chunk to process
   * @param transactionBatchSize the size of transaction batches (used only in transaction mode)
   * @param numCores the number of CPU cores to use for parallel processing
   * @return an {@link ImportDataChunkStatus} containing the complete processing results and metrics
   */
  protected ImportDataChunkStatus processDataChunk(
      ImportDataChunk dataChunk, int transactionBatchSize, int numCores) {
    ImportDataChunkStatus status =
        ImportDataChunkStatus.builder()
            .dataChunkId(dataChunk.getDataChunkId())
            .startTime(Instant.now())
            .status(ImportDataChunkStatusState.IN_PROGRESS)
            .build();
    notifyDataChunkStarted(status);
    ImportDataChunkStatus importDataChunkStatus;
    if (params.getScalarDBMode() == ScalarDBMode.TRANSACTION) {
      importDataChunkStatus =
          processDataChunkWithTransactions(dataChunk, transactionBatchSize, numCores);
    } else {
      importDataChunkStatus = processDataChunkWithoutTransactions(dataChunk, numCores);
    }
    notifyDataChunkCompleted(importDataChunkStatus);
    return importDataChunkStatus;
  }

  /**
   * Processes a data chunk using transaction mode with parallel batch processing. Multiple
   * transaction batches are processed concurrently using a thread pool.
   *
   * @param dataChunk the data chunk to process
   * @param transactionBatchSize the number of records per transaction batch
   * @param numCores the maximum number of concurrent transactions to process
   * @return an {@link ImportDataChunkStatus} containing processing results and metrics
   */
  private ImportDataChunkStatus processDataChunkWithTransactions(
      ImportDataChunk dataChunk, int transactionBatchSize, int numCores) {
    Instant startTime = Instant.now();
    List<ImportTransactionBatch> transactionBatches =
        splitIntoTransactionBatches(dataChunk, transactionBatchSize);
    ExecutorService transactionBatchExecutor =
        Executors.newFixedThreadPool(ConfigUtil.getTransactionBatchThreadPoolSize());
    List<Future<?>> transactionBatchFutures = new ArrayList<>();
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger failureCount = new AtomicInteger(0);
    try {
      for (ImportTransactionBatch transactionBatch : transactionBatches) {
        Future<?> transactionBatchFuture =
            transactionBatchExecutor.submit(
                () -> processTransactionBatch(dataChunk, transactionBatch));
        transactionBatchFutures.add(transactionBatchFuture);
      }

      waitForFuturesToComplete(transactionBatchFutures);
      transactionBatchFutures.forEach(
          batchResult -> {
            try {
              ImportTransactionBatchResult importTransactionBatchResult =
                  (ImportTransactionBatchResult) batchResult.get();
              importTransactionBatchResult
                  .getRecords()
                  .forEach(
                      batchRecords -> {
                        if (batchRecords.getTargets().stream()
                            .allMatch(
                                targetResult ->
                                    targetResult
                                        .getStatus()
                                        .equals(ImportTargetResultStatus.SAVED))) {
                          successCount.incrementAndGet();
                        } else {
                          failureCount.incrementAndGet();
                        }
                      });
            } catch (InterruptedException | ExecutionException e) {
              throw new RuntimeException(e);
            }
          });
    } finally {
      transactionBatchExecutor.shutdown();
    }
    Instant endTime = Instant.now();
    int totalDuration = (int) Duration.between(startTime, endTime).toMillis();
    return ImportDataChunkStatus.builder()
        .dataChunkId(dataChunk.getDataChunkId())
        .failureCount(failureCount.get())
        .successCount(successCount.get())
        .totalRecords(dataChunk.getSourceData().size())
        .batchCount(transactionBatches.size())
        .status(ImportDataChunkStatusState.COMPLETE)
        .startTime(startTime)
        .endTime(endTime)
        .totalDurationInMilliSeconds(totalDuration)
        .build();
  }

  /**
   * Processes a data chunk using storage mode with parallel record processing. Individual records
   * are processed concurrently without transaction guarantees.
   *
   * @param dataChunk the data chunk to process
   * @param numCores the number of records to process concurrently
   * @return an {@link ImportDataChunkStatus} containing processing results and metrics
   */
  private ImportDataChunkStatus processDataChunkWithoutTransactions(
      ImportDataChunk dataChunk, int numCores) {
    Instant startTime = Instant.now();
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger failureCount = new AtomicInteger(0);
    ExecutorService recordExecutor = Executors.newFixedThreadPool(numCores);
    List<Future<?>> recordFutures = new ArrayList<>();
    try {
      for (ImportRow importRow : dataChunk.getSourceData()) {
        Future<?> recordFuture =
            recordExecutor.submit(() -> processStorageRecord(dataChunk, importRow));
        recordFutures.add(recordFuture);
      }
      waitForFuturesToComplete(recordFutures);
      recordFutures.forEach(
          r -> {
            try {
              ImportTaskResult result = (ImportTaskResult) r.get();
              boolean allSaved =
                  result.getTargets().stream()
                      .allMatch(t -> t.getStatus().equals(ImportTargetResultStatus.SAVED));
              if (allSaved) successCount.incrementAndGet();
              else failureCount.incrementAndGet();
            } catch (InterruptedException | ExecutionException e) {
              throw new RuntimeException(e);
            }
          });
    } finally {
      recordExecutor.shutdown();
    }
    Instant endTime = Instant.now();
    int totalDuration = (int) Duration.between(startTime, endTime).toMillis();
    return ImportDataChunkStatus.builder()
        .dataChunkId(dataChunk.getDataChunkId())
        .totalRecords(dataChunk.getSourceData().size())
        .successCount(successCount.get())
        .failureCount(failureCount.get())
        .startTime(startTime)
        .endTime(endTime)
        .totalDurationInMilliSeconds(totalDuration)
        .status(ImportDataChunkStatusState.COMPLETE)
        .build();
  }

  /**
   * Waits for all futures in the provided list to complete. Any exceptions during execution are
   * logged but not propagated.
   *
   * @param futures the list of {@link Future} objects to wait for
   */
  private void waitForFuturesToComplete(List<Future<?>> futures) {
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (Exception e) {
        LOGGER.error(e.getMessage());
      }
    }
  }
}
