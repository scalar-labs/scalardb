package com.scalar.db.dataloader.core.dataimport.log;

import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.db.dataloader.core.Constants;
import com.scalar.db.dataloader.core.DataLoaderObjectMapper;
import com.scalar.db.dataloader.core.dataimport.ImportEventListener;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportDataChunkStatus;
import com.scalar.db.dataloader.core.dataimport.log.writer.LogWriter;
import com.scalar.db.dataloader.core.dataimport.log.writer.LogWriterFactory;
import com.scalar.db.dataloader.core.dataimport.task.result.ImportTargetResult;
import com.scalar.db.dataloader.core.dataimport.task.result.ImportTargetResultStatus;
import com.scalar.db.dataloader.core.dataimport.task.result.ImportTaskResult;
import com.scalar.db.dataloader.core.dataimport.transactionbatch.ImportTransactionBatchResult;
import com.scalar.db.dataloader.core.dataimport.transactionbatch.ImportTransactionBatchStatus;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.RequiredArgsConstructor;

/**
 * An abstract base class for logging import events during data loading operations. This class
 * implements the {@link ImportEventListener} interface and provides common functionality for
 * logging transaction batch results and managing event listeners. Concrete implementations should
 * define how to log transaction batches and handle errors.
 */
@RequiredArgsConstructor
public abstract class AbstractImportLogger implements ImportEventListener {

  /** Object mapper used for JSON serialization/deserialization. */
  protected static final DataLoaderObjectMapper OBJECT_MAPPER = new DataLoaderObjectMapper();

  /** Configuration for the import logger. */
  protected final ImportLoggerConfig config;

  /** Factory for creating log writers. */
  protected final LogWriterFactory logWriterFactory;

  /** List of event listeners to be notified of import events. */
  protected final List<ImportEventListener> listeners = new ArrayList<>();

  /**
   * Called when a data chunk import is started. Currently, this implementation does not log the
   * start of a data chunk.
   *
   * @param importDataChunkStatus the status of the data chunk being imported
   */
  @Override
  public void onDataChunkStarted(ImportDataChunkStatus importDataChunkStatus) {
    // Currently we are not logging the start of a data chunk
  }

  /**
   * Called when a transaction batch is started. Currently, this implementation does not log the
   * start of a transaction batch, but it notifies all registered listeners.
   *
   * @param batchStatus the status of the transaction batch being started
   */
  @Override
  public void onTransactionBatchStarted(ImportTransactionBatchStatus batchStatus) {
    // Currently we are not logging the start of a transaction batch
    notifyTransactionBatchStarted(batchStatus);
  }

  /**
   * Called when a transaction batch is completed. This method logs the transaction batch result if
   * it should be logged based on the configuration, and notifies all registered listeners.
   *
   * @param batchResult the result of the completed transaction batch
   */
  @Override
  public void onTransactionBatchCompleted(ImportTransactionBatchResult batchResult) {
    // skip logging success records if the configuration is set to skip
    if (shouldSkipLoggingSuccess(batchResult)) {
      return;
    }

    logTransactionBatch(batchResult);
    notifyTransactionBatchCompleted(batchResult);
  }

  /**
   * Logs a transaction batch result. This method should be implemented by concrete subclasses to
   * define how to log transaction batch results.
   *
   * @param batchResult the transaction batch result to log
   */
  protected abstract void logTransactionBatch(ImportTransactionBatchResult batchResult);

  /**
   * Determines whether logging of a successful transaction batch should be skipped. Logging is
   * skipped if the batch was successful and the configuration specifies not to log success records.
   *
   * @param batchResult the transaction batch result to check
   * @return true if logging should be skipped, false otherwise
   */
  protected boolean shouldSkipLoggingSuccess(ImportTransactionBatchResult batchResult) {
    return batchResult.isSuccess() && !config.isLogSuccessRecordsEnabled();
  }

  /**
   * Creates a filtered JSON representation of a transaction batch result. This method filters out
   * raw record data if the configuration specifies not to log raw source records.
   *
   * @param batchResult the transaction batch result to convert to JSON
   * @return a JsonNode representing the filtered transaction batch result
   */
  protected JsonNode createFilteredTransactionBatchLogJsonNode(
      ImportTransactionBatchResult batchResult) {

    // If the batch result does not contain any records, return the batch result as is
    if (batchResult.getRecords() == null) {
      return OBJECT_MAPPER.valueToTree(batchResult);
    }

    // Create a new list to store the modified import task results
    List<ImportTaskResult> modifiedRecords = new ArrayList<>();

    // Loop over the records in the batchResult
    for (ImportTaskResult taskResult : batchResult.getRecords()) {
      // Create a new ImportTaskResult and not add the raw record yet
      List<ImportTargetResult> targetResults =
          batchResult.isSuccess()
              ? taskResult.getTargets()
              : updateTargetStatusForAbortedTransactionBatch(taskResult.getTargets());
      ImportTaskResult.ImportTaskResultBuilder builder =
          ImportTaskResult.builder()
              .rowNumber(taskResult.getRowNumber())
              .targets(targetResults)
              .dataChunkId(taskResult.getDataChunkId());

      // Adds the raw record if the configuration is set to log raw source data
      if (config.isLogRawSourceRecordsEnabled()) {
        builder.rawRecord(taskResult.getRawRecord());
      }
      ImportTaskResult modifiedTaskResult = builder.build();

      // Add the modified task result to the list
      modifiedRecords.add(modifiedTaskResult);
    }

    // Create a new transaction batch result with the modified import task results
    ImportTransactionBatchResult modifiedBatchResult =
        ImportTransactionBatchResult.builder()
            .dataChunkId(batchResult.getDataChunkId())
            .transactionBatchId(batchResult.getTransactionBatchId())
            .transactionId(batchResult.getTransactionId())
            .records(modifiedRecords)
            .errors(batchResult.getErrors())
            .success(batchResult.isSuccess())
            .build();

    // Convert the modified batch result to a JsonNode
    return OBJECT_MAPPER.valueToTree(modifiedBatchResult);
  }

  /**
   * Safely closes a log writer. If an IOException occurs during closing, it logs the error using
   * the {@link #logError} method.
   *
   * @param logWriter the log writer to close, may be null
   */
  protected void closeLogWriter(LogWriter logWriter) {
    if (logWriter != null) {
      try {
        logWriter.close();
      } catch (IOException e) {
        logError("Failed to close a log writer", e);
      }
    }
  }

  /**
   * Logs an error message and exception. This method should be implemented by concrete subclasses
   * to define how to log errors.
   *
   * @param errorMessage the error message to log
   * @param e the exception that caused the error
   */
  protected abstract void logError(String errorMessage, Exception e);

  /**
   * Creates a log writer for the specified log file path.
   *
   * @param logFilePath the path to the log file
   * @return a new log writer
   * @throws IOException if an I/O error occurs while creating the log writer
   */
  protected LogWriter createLogWriter(String logFilePath) throws IOException {
    return logWriterFactory.createLogWriter(logFilePath);
  }

  /**
   * Notifies all registered listeners that a transaction batch has started.
   *
   * @param status the status of the transaction batch that has started
   */
  private void notifyTransactionBatchStarted(ImportTransactionBatchStatus status) {
    for (ImportEventListener listener : listeners) {
      listener.onTransactionBatchStarted(status);
    }
  }

  /**
   * Notifies all registered listeners that a transaction batch has completed.
   *
   * @param batchResult the result of the completed transaction batch
   */
  private void notifyTransactionBatchCompleted(ImportTransactionBatchResult batchResult) {
    for (ImportEventListener listener : listeners) {
      listener.onTransactionBatchCompleted(batchResult);
    }
  }

  /**
   * Updates the status of target results for an aborted transaction batch. For each target with a
   * status of SAVED, changes the status to ABORTED and adds an error message.
   *
   * @param targetResults the list of target results to update
   * @return the updated list of target results
   */
  private List<ImportTargetResult> updateTargetStatusForAbortedTransactionBatch(
      List<ImportTargetResult> targetResults) {
    for (int i = 0; i < targetResults.size(); i++) {
      ImportTargetResult target = targetResults.get(i);
      if (target.getStatus().equals(ImportTargetResultStatus.SAVED)) {
        ImportTargetResult newTarget =
            ImportTargetResult.builder()
                .importAction(target.getImportAction())
                .status(ImportTargetResultStatus.ABORTED)
                .importedRecord(target.getImportedRecord())
                .namespace(target.getNamespace())
                .tableName(target.getTableName())
                .dataMapped(target.isDataMapped())
                .errors(Collections.singletonList(Constants.ABORT_TRANSACTION_STATUS))
                .build();
        targetResults.set(i, newTarget);
      }
    }
    return targetResults;
  }
}
