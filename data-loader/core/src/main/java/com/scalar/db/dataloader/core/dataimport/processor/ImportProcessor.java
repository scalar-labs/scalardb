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
import com.scalar.db.exception.transaction.TransactionException;
import java.io.BufferedReader;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequiredArgsConstructor
public abstract class ImportProcessor {

  final ImportProcessorParams params;
  private static final Logger LOGGER = LoggerFactory.getLogger(ImportProcessor.class);
  private final List<ImportEventListener> listeners = new ArrayList<>();

  /**
   * Processes the source data from the given import file.
   *
   * <p>This method reads data from the provided {@link BufferedReader}, processes it in chunks, and
   * batches transactions according to the specified sizes. The method returns a list of {@link
   * ImportDataChunkStatus} objects, each representing the status of a processed data chunk.
   *
   * @param dataChunkSize the number of records to include in each data chunk
   * @param transactionBatchSize the number of records to include in each transaction batch
   * @param reader the {@link BufferedReader} used to read the source file
   * @return a list of {@link ImportDataChunkStatus} objects indicating the processing status of
   *     each data chunk
   */
  public List<ImportDataChunkStatus> process(
      int dataChunkSize, int transactionBatchSize, BufferedReader reader) {
    return Collections.emptyList();
  }

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
   * Split the data chunk into transaction batches
   *
   * @param dataChunk data chunk object
   * @param batchSize batch size
   * @return created list of transaction batches
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
   * To process a transaction batch and return the result
   *
   * @param dataChunk data chunk object
   * @param transactionBatch transaction batch object
   * @return processed transaction batch result
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
    try {
      // Create the ScalarDB transaction
      DistributedTransaction transaction = params.getDistributedTransactionManager().start();

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
   * @param dataChunk data chunk object
   * @param importRow data row object
   * @return thr task result after processing the row data
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
   * Process data chunk data
   *
   * @param dataChunk data chunk object
   * @param transactionBatchSize transaction batch size
   * @param numCores num of cpu cores
   * @return import data chunk status object after processing the data chunk
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
   * Process data chunk data with transactions
   *
   * @param dataChunk data chunk object
   * @param transactionBatchSize transaction batch size
   * @param numCores num of cpu cores
   * @return import data chunk status object after processing the data chunk
   */
  private ImportDataChunkStatus processDataChunkWithTransactions(
      ImportDataChunk dataChunk, int transactionBatchSize, int numCores) {
    Instant startTime = Instant.now();
    List<ImportTransactionBatch> transactionBatches =
        splitIntoTransactionBatches(dataChunk, transactionBatchSize);
    ExecutorService transactionBatchExecutor =
        Executors.newFixedThreadPool(Math.min(transactionBatches.size(), numCores));
    List<Future<?>> transactionBatchFutures = new ArrayList<>();
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger failureCount = new AtomicInteger(0);
    for (ImportTransactionBatch transactionBatch : transactionBatches) {
      Future<?> transactionBatchFuture =
          transactionBatchExecutor.submit(
              () -> processTransactionBatch(dataChunk, transactionBatch));
      transactionBatchFutures.add(transactionBatchFuture);
    }

    waitForFuturesToComplete(transactionBatchFutures);
    transactionBatchExecutor.shutdown();
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
   * Process data chunk data without transactions
   *
   * @param dataChunk data chunk object
   * @param numCores num of cpu cores
   * @return import data chunk status object after processing the data chunk
   */
  private ImportDataChunkStatus processDataChunkWithoutTransactions(
      ImportDataChunk dataChunk, int numCores) {
    Instant startTime = Instant.now();
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger failureCount = new AtomicInteger(0);
    ExecutorService recordExecutor = Executors.newFixedThreadPool(numCores);
    List<Future<?>> recordFutures = new ArrayList<>();
    for (ImportRow importRow : dataChunk.getSourceData()) {
      Future<?> recordFuture =
          recordExecutor.submit(() -> processStorageRecord(dataChunk, importRow));
      recordFutures.add(recordFuture);
    }
    waitForFuturesToComplete(recordFutures);
    recordExecutor.shutdown();
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
