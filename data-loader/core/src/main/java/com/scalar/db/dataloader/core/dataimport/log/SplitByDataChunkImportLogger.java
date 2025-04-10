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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SplitByDataChunkImportLogger extends AbstractImportLogger {

  protected static final String SUMMARY_LOG_FILE_NAME_FORMAT = "data_chunk_%s_summary.json";
  protected static final String FAILURE_LOG_FILE_NAME_FORMAT = "data_chunk_%s_failure.json";
  protected static final String SUCCESS_LOG_FILE_NAME_FORMAT = "data_chunk_%s_success.json";

  private static final Logger LOGGER = LoggerFactory.getLogger(SplitByDataChunkImportLogger.class);
  private final Map<Integer, LogWriter> summaryLogWriters = new HashMap<>();
  private final Map<Integer, LogWriter> successLogWriters = new HashMap<>();
  private final Map<Integer, LogWriter> failureLogWriters = new HashMap<>();

  public SplitByDataChunkImportLogger(
      ImportLoggerConfig config, LogWriterFactory logWriterFactory) {
    super(config, logWriterFactory);
  }

  @Override
  public void onTaskComplete(ImportTaskResult taskResult) {
    if (!config.isLogSuccessRecords() && !config.isLogRawSourceRecords()) return;
    try {
      writeImportTaskResultDetailToLogs(taskResult);
    } catch (IOException e) {
      LOGGER.error("Failed to write success/failure logs");
    }
  }

  private void writeImportTaskResultDetailToLogs(ImportTaskResult importTaskResult)
      throws IOException {
    JsonNode jsonNode;
    for (ImportTargetResult target : importTaskResult.getTargets()) {
      if (config.isLogSuccessRecords()
          && target.getStatus().equals(ImportTargetResultStatus.SAVED)) {
        jsonNode = OBJECT_MAPPER.valueToTree(target);
        synchronized (successLogWriters) {
          LogWriter successLogWriter =
              initializeLogWriterIfNeeded(LogFileType.SUCCESS, importTaskResult.getDataChunkId());
          successLogWriter.write(jsonNode);
          successLogWriter.flush();
        }
      }
      if (config.isLogRawSourceRecords()
          && !target.getStatus().equals(ImportTargetResultStatus.SAVED)) {
        jsonNode = OBJECT_MAPPER.valueToTree(target);
        synchronized (failureLogWriters) {
          LogWriter failureLogWriter =
              initializeLogWriterIfNeeded(LogFileType.FAILURE, importTaskResult.getDataChunkId());
          failureLogWriter.write(jsonNode);
          failureLogWriter.flush();
        }
      }
    }
  }

  @Override
  public void addOrUpdateDataChunkStatus(ImportDataChunkStatus status) {}

  @Override
  public void onDataChunkCompleted(ImportDataChunkStatus dataChunkStatus) {
    try {
      logDataChunkSummary(dataChunkStatus);
      // Close the split log writers per data chunk if they exist for this data chunk id
      closeLogWritersForDataChunk(dataChunkStatus.getDataChunkId());
    } catch (IOException e) {
      LOGGER.error("Failed to log the data chunk summary", e);
    }
  }

  @Override
  public void onAllDataChunksCompleted() {
    closeAllDataChunkLogWriters();
  }

  @Override
  protected void logTransactionBatch(ImportTransactionBatchResult batchResult) {
    LogFileType logFileType = batchResult.isSuccess() ? LogFileType.SUCCESS : LogFileType.FAILURE;
    try (LogWriter logWriter =
        initializeLogWriterIfNeeded(logFileType, batchResult.getDataChunkId())) {
      JsonNode jsonNode = createFilteredTransactionBatchLogJsonNode(batchResult);
      synchronized (logWriter) {
        logWriter.write(jsonNode);
        logWriter.flush();
      }
    } catch (IOException e) {
      LOGGER.error("Failed to write a transaction batch record to a split mode log file", e);
    }
  }

  @Override
  protected void logError(String errorMessage, Exception exception) {
    LOGGER.error(errorMessage, exception);
  }

  private void logDataChunkSummary(ImportDataChunkStatus dataChunkStatus) throws IOException {
    try (LogWriter logWriter =
        initializeLogWriterIfNeeded(LogFileType.SUMMARY, dataChunkStatus.getDataChunkId())) {
      logWriter.write(OBJECT_MAPPER.valueToTree(dataChunkStatus));
      logWriter.flush();
    }
  }

  private void closeLogWritersForDataChunk(int dataChunkId) {
    closeLogWriter(successLogWriters.remove(dataChunkId));
    closeLogWriter(failureLogWriters.remove(dataChunkId));
    closeLogWriter(summaryLogWriters.remove(dataChunkId));
  }

  private void closeAllDataChunkLogWriters() {
    summaryLogWriters.values().forEach(this::closeLogWriter);
    successLogWriters.values().forEach(this::closeLogWriter);
    failureLogWriters.values().forEach(this::closeLogWriter);
    summaryLogWriters.clear();
    successLogWriters.clear();
    failureLogWriters.clear();
  }

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

  private LogWriter initializeLogWriterIfNeeded(LogFileType logFileType, int dataChunkId)
      throws IOException {
    Map<Integer, LogWriter> logWriters = getLogWriters(logFileType);
    if (!logWriters.containsKey(dataChunkId)) {
      LogWriter logWriter = createLogWriter(logFileType, dataChunkId);
      logWriters.put(dataChunkId, logWriter);
    }
    return logWriters.get(dataChunkId);
  }

  private LogWriter createLogWriter(LogFileType logFileType, int dataChunkId) throws IOException {
    String logFilePath = getLogFilePath(dataChunkId, logFileType);
    return createLogWriter(logFilePath);
  }

  private Map<Integer, LogWriter> getLogWriters(LogFileType logFileType) {
    Map<Integer, LogWriter> logWriterMap = null;
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
    }
    return logWriterMap;
  }
}
