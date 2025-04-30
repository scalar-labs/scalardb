package com.scalar.db.dataloader.core.dataimport.log;

import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportDataChunkStatus;
import com.scalar.db.dataloader.core.dataimport.log.writer.LogFileType;
import com.scalar.db.dataloader.core.dataimport.log.writer.LogWriter;
import com.scalar.db.dataloader.core.dataimport.log.writer.LogWriterFactory;
import com.scalar.db.dataloader.core.dataimport.task.result.ImportTargetResult;
import com.scalar.db.dataloader.core.dataimport.task.result.ImportTargetResultStatus;
import com.scalar.db.dataloader.core.dataimport.task.result.ImportTaskResult;
import com.scalar.db.dataloader.core.dataimport.transactionbatch.ImportTransactionBatchResult;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link AbstractImportLogger} that creates separate log files for each data
 * chunk. This logger maintains separate log writers for success, failure, and summary logs for each
 * data chunk, allowing for better organization and easier tracking of import operations by data
 * chunk.
 *
 * <p>The log files are named using the following formats:
 *
 * <ul>
 *   <li>Success logs: data_chunk_[id]_success.json
 *   <li>Failure logs: data_chunk_[id]_failure.json
 *   <li>Summary logs: data_chunk_[id]_summary.json
 * </ul>
 *
 * <p>Log writers are created on demand and closed when their corresponding data chunk is completed.
 */
@ThreadSafe
public class SplitByDataChunkImportLogger extends AbstractImportLogger {

  protected static final String SUMMARY_LOG_FILE_NAME_FORMAT = "data_chunk_%s_summary.json";
  protected static final String FAILURE_LOG_FILE_NAME_FORMAT = "data_chunk_%s_failure.json";
  protected static final String SUCCESS_LOG_FILE_NAME_FORMAT = "data_chunk_%s_success.json";

  private static final Logger logger = LoggerFactory.getLogger(SplitByDataChunkImportLogger.class);
  private final Map<Integer, LogWriter> summaryLogWriters = new ConcurrentHashMap<>();
  private final Map<Integer, LogWriter> successLogWriters = new ConcurrentHashMap<>();
  private final Map<Integer, LogWriter> failureLogWriters = new ConcurrentHashMap<>();

  /**
   * Creates a new instance of SplitByDataChunkImportLogger.
   *
   * @param config the configuration for the logger
   * @param logWriterFactory the factory to create log writers
   */
  public SplitByDataChunkImportLogger(
      ImportLoggerConfig config, LogWriterFactory logWriterFactory) {
    super(config, logWriterFactory);
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
    } catch (IOException e) {
      logError("Failed to write success/failure logs", e);
    }
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
      ImportTargetResultStatus status = target.getStatus();
      if (status.equals(ImportTargetResultStatus.SAVED) && config.isLogSuccessRecords()) {
        writeLog(target, LogFileType.SUCCESS, importTaskResult.getDataChunkId());
      } else if (!status.equals(ImportTargetResultStatus.SAVED) && config.isLogRawSourceRecords()) {
        writeLog(target, LogFileType.FAILURE, importTaskResult.getDataChunkId());
      }
    }
  }

  /**
   * Serializes the given {@link ImportTargetResult} to JSON and writes it to a log file
   * corresponding to the provided {@link LogFileType} and data chunk ID.
   *
   * <p>This method ensures thread-safe access to the underlying {@link LogWriter} by synchronizing
   * on the writer instance. It is safe to call concurrently from multiple threads handling the same
   * or different data chunks.
   *
   * @param target the result of processing a single import target to be logged
   * @param logFileType the type of log file to write to (e.g., SUCCESS or FAILURE)
   * @param dataChunkId the ID of the data chunk associated with the log entry
   * @throws IOException if writing or flushing the log fails
   */
  private void writeLog(ImportTargetResult target, LogFileType logFileType, int dataChunkId)
      throws IOException {
    JsonNode jsonNode = OBJECT_MAPPER.valueToTree(target);
    LogWriter writer = initializeLogWriterIfNeeded(logFileType, dataChunkId);
    writer.write(jsonNode);
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
   * Called when a data chunk is completed. Logs the summary of the data chunk and closes the log
   * writers for that data chunk.
   *
   * @param dataChunkStatus the status of the completed data chunk
   */
  @Override
  public void onDataChunkCompleted(ImportDataChunkStatus dataChunkStatus) {
    try {
      logDataChunkSummary(dataChunkStatus);
      // Close the split log writers per data chunk if they exist for this data chunk id
      closeLogWritersForDataChunk(dataChunkStatus.getDataChunkId());
    } catch (IOException e) {
      logError("Failed to log the data chunk summary", e);
    }
  }

  /** Called when all data chunks are completed. Closes all remaining log writers. */
  @Override
  public void onAllDataChunksCompleted() {
    closeAllDataChunkLogWriters();
  }

  /**
   * Logs a transaction batch result to the appropriate log file based on its success status. The
   * log file is determined by the data chunk ID and whether the batch was successful.
   *
   * @param batchResult the transaction batch result to log
   */
  @Override
  protected void logTransactionBatch(ImportTransactionBatchResult batchResult) {
    LogFileType logFileType = batchResult.isSuccess() ? LogFileType.SUCCESS : LogFileType.FAILURE;
    try {
      LogWriter logWriter = initializeLogWriterIfNeeded(logFileType, batchResult.getDataChunkId());
      JsonNode jsonNode = createFilteredTransactionBatchLogJsonNode(batchResult);
      logWriter.write(jsonNode);
    } catch (IOException e) {
      logError("Failed to write a transaction batch record to a split mode log file", e);
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
   * Logs the summary of a data chunk to a summary log file.
   *
   * @param dataChunkStatus the status of the data chunk to log
   * @throws IOException if an I/O error occurs while writing to the log
   */
  private void logDataChunkSummary(ImportDataChunkStatus dataChunkStatus) throws IOException {
    try (LogWriter logWriter =
        initializeLogWriterIfNeeded(LogFileType.SUMMARY, dataChunkStatus.getDataChunkId())) {
      logWriter.write(OBJECT_MAPPER.valueToTree(dataChunkStatus));
      logWriter.flush();
    }
  }

  /**
   * Closes and removes the log writers for a specific data chunk.
   *
   * @param dataChunkId the ID of the data chunk whose log writers should be closed
   */
  private void closeLogWritersForDataChunk(int dataChunkId) {
    closeLogWriter(successLogWriters.remove(dataChunkId));
    closeLogWriter(failureLogWriters.remove(dataChunkId));
    closeLogWriter(summaryLogWriters.remove(dataChunkId));
  }

  /**
   * Closes all log writers for all data chunks and clears the writer maps. This method is called
   * when all data chunks have been completed.
   */
  private void closeAllDataChunkLogWriters() {
    summaryLogWriters.values().forEach(this::closeLogWriter);
    successLogWriters.values().forEach(this::closeLogWriter);
    failureLogWriters.values().forEach(this::closeLogWriter);
    summaryLogWriters.clear();
    successLogWriters.clear();
    failureLogWriters.clear();
  }

  /**
   * Constructs the log file path based on the batch ID and log file type.
   *
   * @param batchId the ID of the batch (data chunk)
   * @param logFileType the type of log file (SUCCESS, FAILURE, or SUMMARY)
   * @return the full path to the log file
   */
  private String getLogFilePath(long batchId, LogFileType logFileType) {
    String logfilePath;
    switch (logFileType) {
      case SUCCESS:
        logfilePath =
            config.getLogDirectoryPath() + String.format(SUCCESS_LOG_FILE_NAME_FORMAT, batchId);
        break;
      case FAILURE:
        logfilePath =
            config.getLogDirectoryPath() + String.format(FAILURE_LOG_FILE_NAME_FORMAT, batchId);
        break;
      case SUMMARY:
        logfilePath =
            config.getLogDirectoryPath() + String.format(SUMMARY_LOG_FILE_NAME_FORMAT, batchId);
        break;
      default:
        logfilePath = "";
    }
    return logfilePath;
  }

  /**
   * Gets or creates a log writer for the specified log file type and data chunk ID. If a log writer
   * for the specified type and data chunk doesn't exist, it will be created.
   *
   * @param logFileType the type of log file
   * @param dataChunkId the ID of the data chunk
   * @return the log writer for the specified type and data chunk
   * @throws IOException if an I/O error occurs while creating a new log writer
   */
  private LogWriter initializeLogWriterIfNeeded(LogFileType logFileType, int dataChunkId)
      throws IOException {
    Map<Integer, LogWriter> logWriters = getLogWriters(logFileType);
    try {
      return logWriters.computeIfAbsent(
          dataChunkId,
          id -> {
            try {
              return createLogWriter(logFileType, id);
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          });
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }

  /**
   * Creates a new log writer for the specified log file type and data chunk ID.
   *
   * @param logFileType the type of log file
   * @param dataChunkId the ID of the data chunk
   * @return a new log writer
   * @throws IOException if an I/O error occurs while creating the log writer
   */
  private LogWriter createLogWriter(LogFileType logFileType, int dataChunkId) throws IOException {
    String logFilePath = getLogFilePath(dataChunkId, logFileType);
    return createLogWriter(logFilePath);
  }

  /**
   * Gets the appropriate map of log writers for the specified log file type.
   *
   * @param logFileType the type of log file
   * @return the map of log writers for the specified type
   */
  private Map<Integer, LogWriter> getLogWriters(LogFileType logFileType) {
    Map<Integer, LogWriter> logWriterMap;
    switch (logFileType) {
      case SUCCESS:
        logWriterMap = successLogWriters;
        break;
      case FAILURE:
        logWriterMap = failureLogWriters;
        break;
      case SUMMARY:
        logWriterMap = summaryLogWriters;
        break;
      default:
        throw new AssertionError();
    }
    return logWriterMap;
  }
}
