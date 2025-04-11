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

@RequiredArgsConstructor
public abstract class AbstractImportLogger implements ImportEventListener {

  protected static final DataLoaderObjectMapper OBJECT_MAPPER = new DataLoaderObjectMapper();

  protected final ImportLoggerConfig config;
  protected final LogWriterFactory logWriterFactory;
  protected final List<ImportEventListener> listeners = new ArrayList<>();

  @Override
  public void onDataChunkStarted(ImportDataChunkStatus importDataChunkStatus) {
    // Currently we are not logging the start of a data chunk
  }

  @Override
  public void onTransactionBatchStarted(ImportTransactionBatchStatus batchStatus) {
    // Currently we are not logging the start of a transaction batch
    notifyTransactionBatchStarted(batchStatus);
  }

  @Override
  public void onTransactionBatchCompleted(ImportTransactionBatchResult batchResult) {
    // skip logging success records if the configuration is set to skip
    if (shouldSkipLoggingSuccess(batchResult)) {
      return;
    }

    logTransactionBatch(batchResult);
    notifyTransactionBatchCompleted(batchResult);
  }

  protected abstract void logTransactionBatch(ImportTransactionBatchResult batchResult);

  protected boolean shouldSkipLoggingSuccess(ImportTransactionBatchResult batchResult) {
    return batchResult.isSuccess() && !config.isLogSuccessRecords();
  }

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
              .dataChunkId(taskResult.getDataChunkId())
              .rowNumber(taskResult.getRowNumber());

      // Only add the raw record if the configuration is set to log raw source data
      if (config.isLogRawSourceRecords()) {
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

  protected void closeLogWriter(LogWriter logWriter) {
    if (logWriter != null) {
      try {
        logWriter.close();
      } catch (IOException e) {
        logError("Failed to close a log writer", e);
      }
    }
  }

  protected abstract void logError(String errorMessage, Exception e);

  protected LogWriter createLogWriter(String logFilePath) throws IOException {
    return logWriterFactory.createLogWriter(logFilePath);
  }

  private void notifyTransactionBatchStarted(ImportTransactionBatchStatus status) {
    for (ImportEventListener listener : listeners) {
      listener.onTransactionBatchStarted(status);
    }
  }

  private void notifyTransactionBatchCompleted(ImportTransactionBatchResult batchResult) {
    for (ImportEventListener listener : listeners) {
      listener.onTransactionBatchCompleted(batchResult);
    }
  }

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
