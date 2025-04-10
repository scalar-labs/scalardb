package com.scalar.db.dataloader.core.dataimport.log;

import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportDataChunkStatus;
import com.scalar.db.dataloader.core.dataimport.log.writer.LogWriter;
import com.scalar.db.dataloader.core.dataimport.log.writer.LogWriterFactory;
import com.scalar.db.dataloader.core.dataimport.task.result.ImportTargetResult;
import com.scalar.db.dataloader.core.dataimport.task.result.ImportTargetResultStatus;
import com.scalar.db.dataloader.core.dataimport.task.result.ImportTaskResult;
import com.scalar.db.dataloader.core.dataimport.transactionbatch.ImportTransactionBatchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SingleFileImportLogger extends AbstractImportLogger {

  protected static final String SUMMARY_LOG_FILE_NAME = "summary.log";
  protected static final String SUCCESS_LOG_FILE_NAME = "success.json";
  protected static final String FAILURE_LOG_FILE_NAME = "failure.json";
  private static final Logger LOGGER = LoggerFactory.getLogger(SingleFileImportLogger.class);
  private LogWriter summaryLogWriter;
  private LogWriter successLogWriter;
  private LogWriter failureLogWriter;

  public SingleFileImportLogger(ImportLoggerConfig config, LogWriterFactory logWriterFactory)
      throws IOException {
    super(config, logWriterFactory);
    successLogWriter = createLogWriter(config.getLogDirectoryPath() + SUCCESS_LOG_FILE_NAME);
    failureLogWriter = createLogWriter(config.getLogDirectoryPath() + FAILURE_LOG_FILE_NAME);
  }

  @Override
  public void onTaskComplete(ImportTaskResult taskResult) {
    if (!config.isLogSuccessRecords() && !config.isLogRawSourceRecords()) return;
    try {
      writeImportTaskResultDetailToLogs(taskResult);
    } catch (Exception e) {
      logError("Failed to write success/failure logs", e);
    }
  }

  @Override
  public void addOrUpdateDataChunkStatus(ImportDataChunkStatus status) {}

  @Override
  public void onDataChunkCompleted(ImportDataChunkStatus dataChunkStatus) {
    try {
      logDataChunkSummary(dataChunkStatus);
    } catch (IOException e) {
      logError("Failed to log the data chunk summary", e);
    }
  }

  @Override
  public void onAllDataChunksCompleted() {
    closeAllLogWriters();
  }

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

  @Override
  protected void logError(String errorMessage, Exception exception) {
    LOGGER.error(errorMessage, exception);
  }

  private void logDataChunkSummary(ImportDataChunkStatus dataChunkStatus) throws IOException {
    if (summaryLogWriter == null) {
      summaryLogWriter = createLogWriter(config.getLogDirectoryPath() + SUMMARY_LOG_FILE_NAME);
    }
    writeImportDataChunkSummary(dataChunkStatus, summaryLogWriter);
  }

  private void writeImportDataChunkSummary(
      ImportDataChunkStatus dataChunkStatus, LogWriter logWriter) throws IOException {
    JsonNode jsonNode = OBJECT_MAPPER.valueToTree(dataChunkStatus);
    writeToLogWriter(logWriter, jsonNode);
  }

  private LogWriter getLogWriterForTransactionBatch(ImportTransactionBatchResult batchResult)
      throws IOException {
    String logFileName = batchResult.isSuccess() ? SUCCESS_LOG_FILE_NAME : FAILURE_LOG_FILE_NAME;
    LogWriter logWriter = batchResult.isSuccess() ? successLogWriter : failureLogWriter;
    if (logWriter == null) {
      logWriter = createLogWriter(config.getLogDirectoryPath() + logFileName);
      if (batchResult.isSuccess()) {
        successLogWriter = logWriter;
      } else {
        failureLogWriter = logWriter;
      }
    }
    return logWriter;
  }

  private void writeImportTaskResultDetailToLogs(ImportTaskResult importTaskResult)
      throws IOException {
    JsonNode jsonNode;
    for (ImportTargetResult target : importTaskResult.getTargets()) {
      if (config.isLogSuccessRecords()
          && target.getStatus().equals(ImportTargetResultStatus.SAVED)) {
        synchronized (successLogWriter) {
          jsonNode = OBJECT_MAPPER.valueToTree(target);
          successLogWriter.write(jsonNode);
          successLogWriter.flush();
        }
      }
      if (config.isLogRawSourceRecords()
          && !target.getStatus().equals(ImportTargetResultStatus.SAVED)) {
        synchronized (failureLogWriter) {
          jsonNode = OBJECT_MAPPER.valueToTree(target);
          failureLogWriter.write(jsonNode);
          failureLogWriter.flush();
        }
      }
    }
  }

  private void writeToLogWriter(LogWriter logWriter, JsonNode jsonNode) throws IOException {
    logWriter.write(jsonNode);
    logWriter.flush();
  }

  private void closeAllLogWriters() {
    closeLogWriter(summaryLogWriter);
    closeLogWriter(successLogWriter);
    closeLogWriter(failureLogWriter);
    summaryLogWriter = null;
    successLogWriter = null;
    failureLogWriter = null;
  }
}
