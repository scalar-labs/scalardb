package com.scalar.db.dataloader.core.dataimport.processor;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.dataloader.core.TransactionMode;
import com.scalar.db.dataloader.core.dataimport.ImportEventListener;
import com.scalar.db.dataloader.core.dataimport.ImportRow;
import com.scalar.db.dataloader.core.dataimport.ImportStatus;
import com.scalar.db.dataloader.core.dataimport.ImportStatusState;
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
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract class that handles the processing of data imports into ScalarDB. This processor
 * supports both transactional and non-transactional (storage) modes and provides event notification
 * capabilities for monitoring the import process.
 *
 * <p>The processor uses a streaming approach where records are read and processed in batches
 * according to the transaction batch size. This ensures constant memory usage regardless of the
 * total file size.
 */
@SuppressWarnings({"SameNameButDifferent"})
@RequiredArgsConstructor
public abstract class ImportProcessor {

  final ImportProcessorParams params;
  private static final Logger logger = LoggerFactory.getLogger(ImportProcessor.class);
  private final List<ImportEventListener> listeners = new ArrayList<>();

  /**
   * Processes the source data from the given import file in a streaming fashion.
   *
   * <p>This method reads data from the provided {@link BufferedReader} in batches and processes
   * each batch immediately. Records are accumulated until {@code transactionBatchSize} is reached,
   * then processed as a transaction (in CONSENSUS_COMMIT mode) or individually (in SINGLE_CRUD
   * mode). This streaming approach ensures constant memory usage regardless of file size.
   *
   * @param transactionBatchSize the number of records to group together in a single transaction
   *     (only used in consensus commit mode)
   * @param reader the {@link BufferedReader} used to read the source file
   */
  public void process(int transactionBatchSize, BufferedReader reader) {
    int batchId = 0;
    int totalSuccess = 0;
    int totalFailure = 0;
    int totalRecords = 0;
    Instant startTime = Instant.now();

    // Notify import start
    notifyImportStarted(
        ImportStatus.builder()
            .importId(0)
            .startTime(startTime)
            .status(ImportStatusState.IN_PROGRESS)
            .build());

    try {
      List<ImportRow> batch;
      while (!(batch = readNextBatch(reader, transactionBatchSize)).isEmpty()) {
        totalRecords += batch.size();
        if (params.getTransactionMode() == TransactionMode.CONSENSUS_COMMIT) {
          ImportTransactionBatch transactionBatch =
              ImportTransactionBatch.builder()
                  .transactionBatchId(batchId++)
                  .sourceData(batch)
                  .build();
          ImportTransactionBatchResult result = processTransactionBatch(transactionBatch);
          if (result.isSuccess()) {
            totalSuccess += batch.size();
          } else {
            totalFailure += batch.size();
          }
        } else {
          // Single CRUD mode - process each record individually
          for (ImportRow row : batch) {
            ImportTaskResult result = processSingleCrudRecord(row);
            boolean allSaved =
                result.getTargets().stream()
                    .allMatch(t -> t.getStatus().equals(ImportTargetResultStatus.SAVED));
            if (allSaved) {
              totalSuccess++;
            } else {
              totalFailure++;
            }
          }
        }
      }
    } finally {
      Instant endTime = Instant.now();
      int totalDuration = (int) Duration.between(startTime, endTime).toMillis();

      // Notify import complete
      notifyImportCompleted(
          ImportStatus.builder()
              .importId(0)
              .startTime(startTime)
              .endTime(endTime)
              .totalRecords(totalRecords)
              .successCount(totalSuccess)
              .failureCount(totalFailure)
              .batchCount(batchId)
              .totalDurationInMilliSeconds(totalDuration)
              .status(ImportStatusState.COMPLETE)
              .build());

      notifyAllImportsCompleted();
    }
  }

  /**
   * Reads the next batch of records from the input source.
   *
   * <p>This method should be implemented by each processor to handle the specific format of the
   * input data. It reads up to {@code batchSize} records from the reader and returns them as a list
   * of {@link ImportRow} objects.
   *
   * @param reader the BufferedReader containing the data
   * @param batchSize the maximum number of records to read in this batch
   * @return a list of {@link ImportRow} objects, or an empty list if no more records are available
   */
  protected abstract List<ImportRow> readNextBatch(BufferedReader reader, int batchSize);

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
   * Notify once a single CRUD task is completed
   *
   * @param result task result object
   */
  protected void notifySingleCrudRecordCompleted(ImportTaskResult result) {
    // Add data to summary, success logs with/without raw data
    for (ImportEventListener listener : listeners) {
      listener.onTaskComplete(result);
    }
  }

  /**
   * Notify when the import process starts
   *
   * @param status import status object
   */
  protected void notifyImportStarted(ImportStatus status) {
    for (ImportEventListener listener : listeners) {
      listener.onImportStarted(status);
    }
  }

  /**
   * Notify when the import process completes
   *
   * @param status import status object
   */
  protected void notifyImportCompleted(ImportStatus status) {
    for (ImportEventListener listener : listeners) {
      listener.onImportCompleted(status);
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

  /** Notify when all imports are completed */
  protected void notifyAllImportsCompleted() {
    for (ImportEventListener listener : listeners) {
      listener.onAllImportsCompleted();
    }
  }

  /**
   * Processes a single transaction batch. Creates a new transaction, processes all records in the
   * batch, and commits or aborts the transaction based on the success of all operations.
   *
   * @param transactionBatch the batch of records to process in a single transaction
   * @return an {@link ImportTransactionBatchResult} containing the processing results and any
   *     errors
   */
  private ImportTransactionBatchResult processTransactionBatch(
      ImportTransactionBatch transactionBatch) {
    ImportTransactionBatchStatus status =
        ImportTransactionBatchStatus.builder()
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

      // Check and commit the transaction
      if (isSuccess) {
        transaction.commit();
      } else {
        transaction.abort();
        error = "All transactions are aborted";
      }

    } catch (TransactionException e) {
      isSuccess = false;
      logger.error(
          "Transaction failed for batch {}: {}",
          transactionBatch.getTransactionBatchId(),
          e.getMessage(),
          e);
      abortTransactionSafely(transaction);
      error = e.getMessage();
    } catch (Exception e) {
      // Catch unchecked exceptions
      isSuccess = false;
      logger.error(
          "Unexpected exception occurred while processing transaction batch {}.",
          transactionBatch.getTransactionBatchId(),
          e);
      abortTransactionSafely(transaction);
      error = "Unexpected error: " + e.getClass().getSimpleName() + " - " + e.getMessage();
    }
    ImportTransactionBatchResult importTransactionBatchResult =
        ImportTransactionBatchResult.builder()
            .transactionBatchId(transactionBatch.getTransactionBatchId())
            .success(isSuccess)
            .records(importRecordResult)
            .errors(Collections.singletonList(error))
            .build();
    notifyTransactionBatchCompleted(importTransactionBatchResult);
    return importTransactionBatchResult;
  }

  /**
   * Safely aborts the provided distributed transaction. If the transaction is null, this method
   * takes no action. If an exception occurs during the abort operation, it is logged as an error.
   *
   * @param transaction the {@link DistributedTransaction} to be aborted, may be null
   */
  private void abortTransactionSafely(@Nullable DistributedTransaction transaction) {
    try {
      if (transaction != null) {
        transaction.abort();
      }
    } catch (Exception e) {
      logger.error("Failed to abort transaction: {}", e.getMessage(), e);
    }
  }

  /**
   * Processes a single record in single CRUD mode (non-transactional). Each record is processed
   * independently without transaction guarantees.
   *
   * @param importRow the record to process
   * @return an {@link ImportTaskResult} containing the processing result for the record
   */
  private ImportTaskResult processSingleCrudRecord(ImportRow importRow) {
    ImportTaskParams taskParams =
        ImportTaskParams.builder()
            .sourceRecord(importRow.getSourceData())
            .rowNumber(importRow.getRowNumber())
            .importOptions(params.getImportOptions())
            .tableColumnDataTypes(params.getTableColumnDataTypes())
            .tableMetadataByTableName(params.getTableMetadataByTableName())
            .dao(params.getDao())
            .build();
    ImportTaskResult importRecordResult =
        new ImportStorageTask(taskParams, params.getDistributedTransactionManager()).execute();

    ImportTaskResult modifiedTaskResult =
        ImportTaskResult.builder()
            .rowNumber(importRecordResult.getRowNumber())
            .rawRecord(importRecordResult.getRawRecord())
            .targets(importRecordResult.getTargets())
            .build();
    notifySingleCrudRecordCompleted(modifiedTaskResult);
    return modifiedTaskResult;
  }
}
