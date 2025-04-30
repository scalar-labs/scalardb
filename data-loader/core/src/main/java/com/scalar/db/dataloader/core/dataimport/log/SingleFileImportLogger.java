package com.scalar.db.dataloader.core.dataimport.log;

import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportDataChunkStatus;
import com.scalar.db.dataloader.core.dataimport.log.writer.LogWriter;
import com.scalar.db.dataloader.core.dataimport.log.writer.LogWriterFactory;
import com.scalar.db.dataloader.core.dataimport.task.result.ImportTargetResult;
import com.scalar.db.dataloader.core.dataimport.task.result.ImportTargetResultStatus;
import com.scalar.db.dataloader.core.dataimport.task.result.ImportTaskResult;
import com.scalar.db.dataloader.core.dataimport.transactionbatch.ImportTransactionBatchResult;
import java.io.IOException;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link AbstractImportLogger} that uses a single file for each log type.
 * Unlike {@link SplitByDataChunkImportLogger}, this logger creates only three log files: one for
 * successful operations, one for failed operations, and one for summary information, regardless of
 * the number of data chunks processed.
 *
 * <p>The log files are named as follows:
 *
 * <ul>
 *   <li>success.json - Records of successful import operations
 *   <li>failure.json - Records of failed import operations
 *   <li>summary.log - Summary information for all data chunks
 * </ul>
 */
@ThreadSafe
public class SingleFileImportLogger extends AbstractImportLogger {

  protected static final String SUMMARY_LOG_FILE_NAME = "summary.log";
  protected static final String SUCCESS_LOG_FILE_NAME = "success.json";
  protected static final String FAILURE_LOG_FILE_NAME = "failure.json";
  private static final Logger logger = LoggerFactory.getLogger(SingleFileImportLogger.class);
  private volatile LogWriter summaryLogWriter;
  private final LogWriter successLogWriter;
  private final LogWriter failureLogWriter;

  /**
   * Creates a new instance of SingleFileImportLogger. Initializes the success and failure log
   * writers immediately. The summary log writer is created on demand when the first data chunk is
   * completed.
   *
   * @param config the configuration for the logger
   * @param logWriterFactory the factory to create log writers
   * @throws IOException if an I/O error occurs while creating the log writers
   */
  public SingleFileImportLogger(ImportLoggerConfig config, LogWriterFactory logWriterFactory)
      throws IOException {
    super(config, logWriterFactory);
    successLogWriter = createLogWriter(config.getLogDirectoryPath() + SUCCESS_LOG_FILE_NAME);
    failureLogWriter = createLogWriter(config.getLogDirectoryPath() + FAILURE_LOG_FILE_NAME);
  }

  /**
   * Called when an import task is completed. Writes the task result details to the appropriate log
   * files based on the configuration.
   *
   * @param taskResult the result of the completed import task
   */
  @Override
  public void onTaskComplete(ImportTaskResult taskResult) {
    if (!config.isLogSuccessRecords() && !config.isLogRawSourceRecords()) return;
    try {
      writeImportTaskResultDetailToLogs(taskResult);
    } catch (Exception e) {
      logError("Failed to write success/failure logs", e);
    }
  }

  /**
   * Called to add or update the status of a data chunk. This implementation does nothing as the
   * status is only logged when the data chunk is completed.
   *
   * @param status the status of the data chunk
   */
  @Override
  public void addOrUpdateDataChunkStatus(ImportDataChunkStatus status) {}

  /**
   * Called when a data chunk is completed. Logs the summary of the data chunk to the summary log
   * file.
   *
   * @param dataChunkStatus the status of the completed data chunk
   */
  @Override
  public void onDataChunkCompleted(ImportDataChunkStatus dataChunkStatus) {
    try {
      logDataChunkSummary(dataChunkStatus);
    } catch (IOException e) {
      logError("Failed to log the data chunk summary", e);
    }
  }

  /** Called when all data chunks are completed. Closes all log writers. */
  @Override
  public void onAllDataChunksCompleted() {
    closeAllLogWriters();
  }

  /**
   * Logs a transaction batch result to the appropriate log file based on its success status.
   *
   * @param batchResult the transaction batch result to log
   */
  @Override
  protected void logTransactionBatch(ImportTransactionBatchResult batchResult) {
    try {
      LogWriter logWriter = getLogWriterForTransactionBatch(batchResult);
      JsonNode jsonNode = createFilteredTransactionBatchLogJsonNode(batchResult);
      writeToLogWriter(logWriter, jsonNode);
    } catch (IOException e) {
      logError("Failed to write a transaction batch record to the log file", e);
    }
  }

  /**
   * Logs an error message with an exception to the logger.
   *
   * @param errorMessage the error message to log
   * @param exception the exception associated with the error
   */
  @Override
  protected void logError(String errorMessage, Exception exception) {
    logger.error(errorMessage, exception);
    throw new RuntimeException(errorMessage, exception);
  }

  /**
   * Logs the summary of a data chunk to the summary log file. Creates the summary log writer if it
   * doesn't exist yet.
   *
   * @param dataChunkStatus the status of the data chunk to log
   * @throws IOException if an I/O error occurs while writing to the log
   */
  private void logDataChunkSummary(ImportDataChunkStatus dataChunkStatus) throws IOException {
    ensureSummaryLogWriterInitialized();
    writeImportDataChunkSummary(dataChunkStatus, summaryLogWriter);
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
   * Writes the summary of a data chunk to the specified log writer.
   *
   * @param dataChunkStatus the status of the data chunk to log
   * @param logWriter the log writer to write to
   * @throws IOException if an I/O error occurs while writing to the log
   */
  private void writeImportDataChunkSummary(
      ImportDataChunkStatus dataChunkStatus, LogWriter logWriter) throws IOException {
    JsonNode jsonNode = OBJECT_MAPPER.valueToTree(dataChunkStatus);
    writeToLogWriter(logWriter, jsonNode);
  }

  /**
   * Gets the appropriate log writer for a transaction batch based on its success status. If the log
   * writer doesn't exist yet, it will be created.
   *
   * @param batchResult the transaction batch result
   * @return the log writer for the batch
   * @throws IOException if an I/O error occurs while creating a new log writer
   */
  private LogWriter getLogWriterForTransactionBatch(ImportTransactionBatchResult batchResult)
      throws IOException {
    return batchResult.isSuccess() ? successLogWriter : failureLogWriter;
  }

  /**
   * Writes the details of an import task result to the appropriate log files. Successful targets
   * are written to success logs and failed targets to failure logs. The method is synchronized on
   * the respective log writers to ensure thread safety.
   *
   * @param importTaskResult the result of the import task to log
   * @throws IOException if an I/O error occurs while writing to the logs
   */
  private void writeImportTaskResultDetailToLogs(ImportTaskResult importTaskResult)
      throws IOException {
    for (ImportTargetResult target : importTaskResult.getTargets()) {
      if (config.isLogSuccessRecords()
          && target.getStatus().equals(ImportTargetResultStatus.SAVED)) {

        writeToLogWriter(successLogWriter, OBJECT_MAPPER.valueToTree(target));
      }
      if (config.isLogRawSourceRecords()
          && !target.getStatus().equals(ImportTargetResultStatus.SAVED)) {
        writeToLogWriter(failureLogWriter, OBJECT_MAPPER.valueToTree(target));
      }
    }
  }

  /**
   * Writes a JSON node to a log writer and flushes the writer.
   *
   * @param logWriter the log writer to write to
   * @param jsonNode the JSON node to write
   * @throws IOException if an I/O error occurs while writing
   */
  private void writeToLogWriter(LogWriter logWriter, JsonNode jsonNode) throws IOException {
    logWriter.write(jsonNode);
  }

  /**
   * Closes all log writers and sets them to null. This method is called when all data chunks have
   * been completed.
   */
  private void closeAllLogWriters() {
    synchronized (this) {
      closeLogWriter(summaryLogWriter);
      closeLogWriter(successLogWriter);
      closeLogWriter(failureLogWriter);
      summaryLogWriter = null;
    }
  }
}
