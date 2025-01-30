package com.scalar.db.dataloader.core.dataimport;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.ScalarDBMode;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDBDao;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportDataChunkStatus;
import com.scalar.db.dataloader.core.dataimport.processor.ImportProcessor;
import com.scalar.db.dataloader.core.dataimport.processor.ImportProcessorFactory;
import com.scalar.db.dataloader.core.dataimport.processor.ImportProcessorParams;
import com.scalar.db.dataloader.core.dataimport.processor.TableColumnDataTypes;
import com.scalar.db.dataloader.core.dataimport.task.result.ImportTaskResult;
import com.scalar.db.dataloader.core.dataimport.transactionbatch.ImportTransactionBatchResult;
import com.scalar.db.dataloader.core.dataimport.transactionbatch.ImportTransactionBatchStatus;
import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.NonNull;

@AllArgsConstructor
public class ImportManager implements ImportEventListener {

  @NonNull private final Map<String, TableMetadata> tableMetadata;
  @NonNull private final BufferedReader importFileReader;
  @NonNull private final ImportOptions importOptions;
  private final ImportProcessorFactory importProcessorFactory;
  private final List<ImportEventListener> listeners = new ArrayList<>();
  private final ScalarDBMode scalarDBMode;
  private final DistributedStorage distributedStorage;
  private final DistributedTransactionManager distributedTransactionManager;
  private final List<ImportDataChunkStatus> importDataChunkStatusList = new ArrayList<>();

  /**
   * * Start the import process
   *
   * @return list of import data chunk status objects
   */
  public List<ImportDataChunkStatus> startImport() {
    ImportProcessorParams params =
        ImportProcessorParams.builder()
            .scalarDBMode(scalarDBMode)
            .importOptions(importOptions)
            .tableMetadataByTableName(tableMetadata)
            .dao(new ScalarDBDao())
            .distributedTransactionManager(distributedTransactionManager)
            .distributedStorage(distributedStorage)
            .tableColumnDataTypes(getTableColumnDataTypes())
            .build();
    ImportProcessor processor = importProcessorFactory.createImportProcessor(params);
    processor.addListener(this);
    // If the data chunk size is 0, then process the entire file in a single data chunk
    int dataChunkSize =
        importOptions.getDataChunkSize() == 0
            ? Integer.MAX_VALUE
            : importOptions.getDataChunkSize();
    return processor.process(
        dataChunkSize, importOptions.getTransactionBatchSize(), importFileReader);
  }

  public void addListener(ImportEventListener listener) {
    listeners.add(listener);
  }

  public void removeListener(ImportEventListener listener) {
    listeners.remove(listener);
  }

  @Override
  public void onDataChunkStarted(ImportDataChunkStatus status) {
    for (ImportEventListener listener : listeners) {
      listener.onDataChunkStarted(status);
    }
  }

  @Override
  public void addOrUpdateDataChunkStatus(ImportDataChunkStatus status) {
    synchronized (importDataChunkStatusList) {
      for (int i = 0; i < importDataChunkStatusList.size(); i++) {
        if (importDataChunkStatusList.get(i).getDataChunkId() == status.getDataChunkId()) {
          // Object found, replace it with the new one
          importDataChunkStatusList.set(i, status);
          return;
        }
      }
      // If object is not found, add it to the list
      importDataChunkStatusList.add(status);
    }
  }

  @Override
  public void onDataChunkCompleted(ImportDataChunkStatus status) {
    for (ImportEventListener listener : listeners) {
      listener.onDataChunkCompleted(status);
    }
  }

  @Override
  public void onTransactionBatchStarted(ImportTransactionBatchStatus status) {
    for (ImportEventListener listener : listeners) {
      listener.onTransactionBatchStarted(status);
    }
  }

  @Override
  public void onTransactionBatchCompleted(ImportTransactionBatchResult batchResult) {
    for (ImportEventListener listener : listeners) {
      listener.onTransactionBatchCompleted(batchResult);
    }
  }

  @Override
  public void onTaskComplete(ImportTaskResult taskResult) {
    for (ImportEventListener listener : listeners) {
      listener.onTaskComplete(taskResult);
    }
  }

  @Override
  public void onAllDataChunksCompleted() {
    for (ImportEventListener listener : listeners) {
      listener.onAllDataChunksCompleted();
    }
  }

  public List<ImportDataChunkStatus> getImportDataChunkStatusList() {
    return importDataChunkStatusList;
  }

  public TableColumnDataTypes getTableColumnDataTypes() {
    TableColumnDataTypes tableColumnDataTypes = new TableColumnDataTypes();
    tableMetadata.forEach(
        (name, metadata) ->
            metadata
                .getColumnDataTypes()
                .forEach((k, v) -> tableColumnDataTypes.addColumnDataType(name, k, v)));
    return tableColumnDataTypes;
  }
}
