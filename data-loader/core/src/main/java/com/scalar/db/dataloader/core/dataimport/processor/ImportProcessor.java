package com.scalar.db.dataloader.core.dataimport.processor;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.ScalarDbMode;
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
import com.scalar.db.exception.transaction.TransactionException;
import java.io.BufferedReader;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Phaser;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
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
   * transactional or storage mode, depending on the configured {@link ScalarDbMode}.
   *
   * @param dataChunkSize the number of records to include in each data chunk for parallel
   *     processing
   * @param transactionBatchSize the number of records to group together in a single transaction
   *     (only used in transaction mode)
   * @param reader the {@link BufferedReader} used to read the source file
   */
  public void process(int dataChunkSize, int transactionBatchSize, BufferedReader reader) {
    ExecutorService dataChunkReaderExecutor = Executors.newSingleThreadExecutor();
    ExecutorService dataChunkProcessorExecutor =
        Executors.newFixedThreadPool(params.getImportOptions().getMaxThreads());
    BlockingQueue<ImportDataChunk> dataChunkQueue =
        new LinkedBlockingQueue<>(params.getImportOptions().getDataChunkQueueSize());

    // Semaphore controls concurrent task submissions, small buffer to be two times of threads
    Semaphore taskSemaphore = new Semaphore(params.getImportOptions().getMaxThreads() * 2);
    // Phaser tracks task completion (start with 1 for the main thread)
    Phaser phaser = new Phaser(1);

    try {
      CompletableFuture<Void> readerFuture =
          CompletableFuture.runAsync(
              () -> readDataChunks(reader, dataChunkSize, dataChunkQueue), dataChunkReaderExecutor);

      while (!(dataChunkQueue.isEmpty() && readerFuture.isDone())) {
        ImportDataChunk dataChunk = dataChunkQueue.poll(100, TimeUnit.MILLISECONDS);
        if (dataChunk != null) {
          // Acquire semaphore permit (blocks if no permits available)
          taskSemaphore.acquire();
          // Register with phaser before submitting
          phaser.register();

          dataChunkProcessorExecutor.submit(
              () -> {
                try {
                  processDataChunk(dataChunk, transactionBatchSize);
                } finally {
                  // Always release semaphore and arrive at phaser
                  taskSemaphore.release();
                  phaser.arriveAndDeregister();
                }
              });
        }
      }

      readerFuture.join();
      // Wait for all tasks to complete
      phaser.arriveAndAwaitAdvance();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(
          CoreError.DATA_LOADER_DATA_CHUNK_PROCESS_FAILED.buildMessage(e.getMessage()), e);
    } finally {
      shutdownExecutorGracefully(dataChunkReaderExecutor);
      shutdownExecutorGracefully(dataChunkProcessorExecutor);
      notifyAllDataChunksCompleted();
    }
  }

  /**
   * Shuts down the given `ExecutorService` gracefully. This method attempts to cleanly shut down
   * the executor by first invoking `shutdown` and waiting for termination for up to 60 seconds. If
   * the executor does not terminate within this time, it forces a shutdown using `shutdownNow`. If
   * interrupted, it forces a shutdown and interrupts the current thread.
   *
   * @param es the `ExecutorService` to be shut down gracefully
   */
  private void shutdownExecutorGracefully(ExecutorService es) {
    es.shutdown();
    try {
      if (!es.awaitTermination(60, TimeUnit.SECONDS)) {
        es.shutdownNow();
      }
    } catch (InterruptedException e) {
      es.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Reads and processes data in chunks from the provided reader.
   *
   * <p>This method should be implemented by each processor to handle the specific format of the
   * input data. It reads data from the reader, converts it to the appropriate format, and enqueues
   * it for processing.
   *
   * @param reader the BufferedReader containing the data
   * @param dataChunkSize the number of rows to include in each chunk
   * @param dataChunkQueue the queue where data chunks are placed for processing
   * @throws RuntimeException if there are errors reading the file or if interrupted
   */
  protected abstract void readDataChunks(
      BufferedReader reader, int dataChunkSize, BlockingQueue<ImportDataChunk> dataChunkQueue);

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
   * @param dataChunkId the parent data chunk id of the chunk containing this batch
   * @param transactionBatch the batch of records to process in a single transaction
   * @return an {@link ImportTransactionBatchResult} containing the processing results and any
   *     errors
   */
  private ImportTransactionBatchResult processTransactionBatch(
      int dataChunkId, ImportTransactionBatch transactionBatch) {
    ImportTransactionBatchStatus status =
        ImportTransactionBatchStatus.builder()
            .dataChunkId(dataChunkId)
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
                .dataChunkId(dataChunkId)
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
            .dataChunkId(dataChunkId)
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
   * @param dataChunkId the parent data chunk id of the chunk containing this record
   * @param importRow the record to process
   * @return an {@link ImportTaskResult} containing the processing result for the record
   */
  private ImportTaskResult processStorageRecord(int dataChunkId, ImportRow importRow) {
    ImportTaskParams taskParams =
        ImportTaskParams.builder()
            .sourceRecord(importRow.getSourceData())
            .dataChunkId(dataChunkId)
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
            .dataChunkId(dataChunkId)
            .build();
    notifyStorageRecordCompleted(modifiedTaskResult);
    return modifiedTaskResult;
  }

  /**
   * Processes a complete data chunk using parallel execution. The processing mode (transactional or
   * storage) is determined by the configured {@link ScalarDbMode}.
   *
   * @param dataChunk the data chunk to process
   * @param transactionBatchSize the size of transaction batches (used only in transaction mode)
   */
  private void processDataChunk(ImportDataChunk dataChunk, int transactionBatchSize) {
    ImportDataChunkStatus status =
        ImportDataChunkStatus.builder()
            .dataChunkId(dataChunk.getDataChunkId())
            .startTime(Instant.now())
            .status(ImportDataChunkStatusState.IN_PROGRESS)
            .build();
    notifyDataChunkStarted(status);
    ImportDataChunkStatus importDataChunkStatus;
    if (params.getScalarDbMode() == ScalarDbMode.TRANSACTION) {
      importDataChunkStatus = processDataChunkWithTransactions(dataChunk, transactionBatchSize);
    } else {
      importDataChunkStatus = processDataChunkWithoutTransactions(dataChunk);
    }
    notifyDataChunkCompleted(importDataChunkStatus);
  }

  /**
   * Processes a data chunk using transaction mode with parallel batch processing. Multiple
   * transaction batches are processed concurrently using a thread pool.
   *
   * @param dataChunk the data chunk to process
   * @param transactionBatchSize the number of records per transaction batch
   * @return an {@link ImportDataChunkStatus} containing processing results and metrics
   */
  private ImportDataChunkStatus processDataChunkWithTransactions(
      ImportDataChunk dataChunk, int transactionBatchSize) {
    Instant startTime = Instant.now();
    List<ImportTransactionBatch> transactionBatches =
        splitIntoTransactionBatches(dataChunk, transactionBatchSize);
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger failureCount = new AtomicInteger(0);

    for (ImportTransactionBatch transactionBatch : transactionBatches) {
      ImportTransactionBatchResult importTransactionBatchResult =
          processTransactionBatch(dataChunk.getDataChunkId(), transactionBatch);

      importTransactionBatchResult
          .getRecords()
          .forEach(
              batchRecords -> {
                if (batchRecords.getTargets().stream()
                    .allMatch(
                        targetResult ->
                            targetResult.getStatus().equals(ImportTargetResultStatus.SAVED))) {
                  successCount.incrementAndGet();
                } else {
                  failureCount.incrementAndGet();
                }
              });
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
   * @return an {@link ImportDataChunkStatus} containing processing results and metrics
   */
  private ImportDataChunkStatus processDataChunkWithoutTransactions(ImportDataChunk dataChunk) {
    Instant startTime = Instant.now();
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger failureCount = new AtomicInteger(0);

    for (ImportRow importRow : dataChunk.getSourceData()) {
      ImportTaskResult result = processStorageRecord(dataChunk.getDataChunkId(), importRow);
      boolean allSaved =
          result.getTargets().stream()
              .allMatch(t -> t.getStatus().equals(ImportTargetResultStatus.SAVED));
      if (allSaved) {
        successCount.incrementAndGet();
      } else {
        failureCount.incrementAndGet();
      }
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
}
