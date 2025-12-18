package com.scalar.db.dataloader.core.dataimport.log;

import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.db.dataloader.core.Constants;
import com.scalar.db.dataloader.core.DataLoaderObjectMapper;
import com.scalar.db.dataloader.core.dataimport.ImportEventListener;
import com.scalar.db.dataloader.core.dataimport.ImportStatus;
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
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A file-based implementation of {@link ImportEventListener} that logs import events to files. This
 * logger creates three log files: one for successful operations, one for failed operations, and one
 * for summary information.
 *
 * <p>The log files are named as follows:
 *
 * <ul>
 *   <li>success.json - Records of successful import operations
 *   <li>failure.json - Records of failed import operations
 *   <li>summary.log - Summary information for the import
 * </ul>
 */
@ThreadSafe
public class ImportFileLogger implements ImportEventListener {

  /** Name for the file where the summary of the import process will be logged. */
  protected static final String SUMMARY_LOG_FILE_NAME = "summary.log";

  /** Name for the file where successfully imported records will be logged in JSON format. */
  protected static final String SUCCESS_LOG_FILE_NAME = "success.json";

  /** Name for the file where failed import records will be logged in JSON format. */
  protected static final String FAILURE_LOG_FILE_NAME = "failure.json";

  /** Object mapper used for JSON serialization/deserialization. */
  protected static final DataLoaderObjectMapper OBJECT_MAPPER = new DataLoaderObjectMapper();

  private static final Logger logger = LoggerFactory.getLogger(ImportFileLogger.class);

  /** Configuration for the import logger. */
  protected final ImportLoggerConfig config;

  /** Factory for creating log writers. */
  protected final LogWriterFactory logWriterFactory;

  /** List of event listeners to be notified of import events. */
  protected final List<ImportEventListener> listeners = new ArrayList<>();

  private volatile LogWriter summaryLogWriter;
  private final LogWriter successLogWriter;
  private final LogWriter failureLogWriter;

  /**
   * Creates a new instance of ImportFileLogger. Initializes the success and failure log writers
   * immediately. The summary log writer is created on demand when the first import is completed.
   *
   * @param config the configuration for the logger
   * @param logWriterFactory the factory to create log writers
   * @throws IOException if an I/O error occurs while creating the log writers
   */
  public ImportFileLogger(ImportLoggerConfig config, LogWriterFactory logWriterFactory)
      throws IOException {
    this.config = config;
    this.logWriterFactory = logWriterFactory;
    successLogWriter = createLogWriter(config.getLogDirectoryPath() + SUCCESS_LOG_FILE_NAME);
    failureLogWriter = createLogWriter(config.getLogDirectoryPath() + FAILURE_LOG_FILE_NAME);
  }

  /**
   * Adds an event listener to be notified of import events.
   *
   * @param listener the listener to add
   */
  public void addListener(ImportEventListener listener) {
    listeners.add(listener);
  }

  /**
   * Called when an import is started. Currently, this implementation does not log the start of an
   * import.
   *
   * @param importStatus the status of the import being processed
   */
  @Override
  public void onImportStarted(ImportStatus importStatus) {
    // Currently we are not logging the start of an import
  }

  /**
   * Called when an import is completed. Logs the summary of the import to the summary log file.
   *
   * @param importStatus the status of the completed import
   */
  @Override
  public void onImportCompleted(ImportStatus importStatus) {
    try {
      logImportSummary(importStatus);
    } catch (IOException e) {
      logError("Failed to log the import summary", e);
    }
  }

  /** Called when all imports are completed. Closes all log writers. */
  @Override
  public void onAllImportsCompleted() {
    closeAllLogWriters();
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
   * Called when an import task is completed. Writes the task result details to the appropriate log
   * files based on the configuration.
   *
   * @param taskResult the result of the completed import task
   */
  @Override
  public void onTaskComplete(ImportTaskResult taskResult) {
    try {
      writeImportTaskResultDetailToLogs(taskResult);
    } catch (Exception e) {
      logError("Failed to write success/failure logs", e);
    }
  }

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
   * Determines whether logging of a successful transaction batch should be skipped. Logging is
   * skipped if the batch was successful and the configuration specifies not to log success records.
   *
   * @param batchResult the transaction batch result to check
   * @return true if logging should be skipped, false otherwise
   */
  private boolean shouldSkipLoggingSuccess(ImportTransactionBatchResult batchResult) {
    return batchResult.isSuccess() && !config.isLogSuccessRecordsEnabled();
  }

  /**
   * Logs a transaction batch result to the appropriate log file based on its success status.
   *
   * @param batchResult the transaction batch result to log
   */
  private void logTransactionBatch(ImportTransactionBatchResult batchResult) {
    try {
      LogWriter logWriter = getLogWriterForTransactionBatch(batchResult);
      JsonNode jsonNode = createFilteredTransactionBatchLogJsonNode(batchResult);
      writeToLogWriter(logWriter, jsonNode);
    } catch (IOException e) {
      logError("Failed to write a transaction batch record to the log file", e);
    }
  }

  /**
   * Logs an error message with an exception to the logger and throws a RuntimeException.
   *
   * @param errorMessage the error message to log
   * @param exception the exception associated with the error
   */
  private void logError(String errorMessage, Exception exception) {
    logger.error(errorMessage, exception);
    throw new RuntimeException(errorMessage, exception);
  }

  /**
   * Creates a filtered JSON representation of a transaction batch result. This method filters out
   * raw record data if the configuration specifies not to log raw source records.
   *
   * @param batchResult the transaction batch result to convert to JSON
   * @return a JsonNode representing the filtered transaction batch result
   */
  private JsonNode createFilteredTransactionBatchLogJsonNode(
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
          ImportTaskResult.builder().rowNumber(taskResult.getRowNumber()).targets(targetResults);

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

  /**
   * Safely closes a log writer. If an IOException occurs during closing, it logs the error.
   *
   * @param logWriter the log writer to close, may be null
   */
  private void closeLogWriter(LogWriter logWriter) {
    if (logWriter != null) {
      try {
        logWriter.close();
      } catch (IOException e) {
        logger.error("Failed to close a log writer", e);
      }
    }
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
   * Logs the summary of an import to the summary log file. Creates the summary log writer if it
   * doesn't exist yet.
   *
   * @param importStatus the status of the import to log
   * @throws IOException if an I/O error occurs while writing to the log
   */
  private void logImportSummary(ImportStatus importStatus) throws IOException {
    ensureSummaryLogWriterInitialized();
    writeImportSummary(importStatus, summaryLogWriter);
  }

  /**
   * Ensures that the summary log writer is initialized in a thread-safe manner.
   *
   * @throws IOException if an error occurs while creating the log writer
   */
  private void ensureSummaryLogWriterInitialized() throws IOException {
    if (summaryLogWriter == null) {
      synchronized (this) {
        if (summaryLogWriter == null) {
          summaryLogWriter = createLogWriter(config.getLogDirectoryPath() + SUMMARY_LOG_FILE_NAME);
        }
      }
    }
  }

  /**
   * Writes the summary of an import to the specified log writer.
   *
   * @param importStatus the status of the import to log
   * @param logWriter the log writer to write to
   * @throws IOException if an I/O error occurs while writing to the log
   */
  private void writeImportSummary(ImportStatus importStatus, LogWriter logWriter)
      throws IOException {
    JsonNode jsonNode = OBJECT_MAPPER.valueToTree(importStatus);
    writeToLogWriter(logWriter, jsonNode);
  }

  /**
   * Gets the appropriate log writer for a transaction batch based on its success status.
   *
   * @param batchResult the transaction batch result
   * @return the log writer for the batch
   */
  private LogWriter getLogWriterForTransactionBatch(ImportTransactionBatchResult batchResult) {
    return batchResult.isSuccess() ? successLogWriter : failureLogWriter;
  }

  /**
   * Writes the details of an import task result to the appropriate log files. Successful targets
   * are written to success logs and failed targets to failure logs.
   *
   * @param importTaskResult the result of the import task to log
   * @throws IOException if an I/O error occurs while writing to the logs
   */
  private void writeImportTaskResultDetailToLogs(ImportTaskResult importTaskResult)
      throws IOException {
    for (ImportTargetResult target : importTaskResult.getTargets()) {
      if (config.isLogSuccessRecordsEnabled()
          && target.getStatus().equals(ImportTargetResultStatus.SAVED)) {

        writeToLogWriter(successLogWriter, OBJECT_MAPPER.valueToTree(target));
      } else if (!target.getStatus().equals(ImportTargetResultStatus.SAVED)) {
        writeToLogWriter(failureLogWriter, OBJECT_MAPPER.valueToTree(target));
      }
    }
  }

  /**
   * Writes a JSON node to a log writer.
   *
   * @param logWriter the log writer to write to
   * @param jsonNode the JSON node to write
   * @throws IOException if an I/O error occurs while writing
   */
  private void writeToLogWriter(LogWriter logWriter, JsonNode jsonNode) throws IOException {
    logWriter.write(jsonNode);
  }

  /** Closes all log writers. This method is called when all imports have been completed. */
  private void closeAllLogWriters() {
    synchronized (this) {
      closeLogWriter(summaryLogWriter);
      closeLogWriter(successLogWriter);
      closeLogWriter(failureLogWriter);
    }
  }
}
