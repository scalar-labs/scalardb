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
import java.util.concurrent.ExecutionException;
import lombok.AllArgsConstructor;
import lombok.NonNull;

/**
 * Manages the data import process and coordinates event handling between the import processor and
 * listeners. This class implements {@link ImportEventListener} to receive events from the processor
 * and relay them to registered listeners.
 *
 * <p>The import process involves:
 *
 * <ul>
 *   <li>Reading data from an input file
 *   <li>Processing the data in configurable chunk sizes
 *   <li>Managing database transactions in batches
 *   <li>Notifying listeners of various import events
 * </ul>
 */
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
   * Starts the import process using the configured parameters.
   *
   * <p>If the data chunk size in {@link ImportOptions} is set to 0, the entire file will be
   * processed as a single chunk. Otherwise, the file will be processed in chunks of the specified
   * size.
   *
   * @return a list of {@link ImportDataChunkStatus} objects containing the status of each processed
   *     chunk
   * @throws ExecutionException if there is an error during the execution of the import process
   * @throws InterruptedException if the import process is interrupted
   */
  public List<ImportDataChunkStatus> startImport() throws ExecutionException, InterruptedException {
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

  /**
   * Registers a new listener to receive import events.
   *
   * @param listener the listener to add
   * @throws IllegalArgumentException if the listener is null
   */
  public void addListener(ImportEventListener listener) {
    listeners.add(listener);
  }

  /**
   * Removes a previously registered listener.
   *
   * @param listener the listener to remove
   */
  public void removeListener(ImportEventListener listener) {
    listeners.remove(listener);
  }

  /** {@inheritDoc} Forwards the event to all registered listeners. */
  @Override
  public void onDataChunkStarted(ImportDataChunkStatus status) {
    for (ImportEventListener listener : listeners) {
      listener.onDataChunkStarted(status);
    }
  }

  /**
   * {@inheritDoc} Updates or adds the status of a data chunk in the status list. This method is
   * thread-safe.
   */
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

  /** {@inheritDoc} Forwards the event to all registered listeners. */
  @Override
  public void onDataChunkCompleted(ImportDataChunkStatus status) {
    for (ImportEventListener listener : listeners) {
      listener.onDataChunkCompleted(status);
    }
  }

  /** {@inheritDoc} Forwards the event to all registered listeners. */
  @Override
  public void onTransactionBatchStarted(ImportTransactionBatchStatus status) {
    for (ImportEventListener listener : listeners) {
      listener.onTransactionBatchStarted(status);
    }
  }

  /** {@inheritDoc} Forwards the event to all registered listeners. */
  @Override
  public void onTransactionBatchCompleted(ImportTransactionBatchResult batchResult) {
    for (ImportEventListener listener : listeners) {
      listener.onTransactionBatchCompleted(batchResult);
    }
  }

  /** {@inheritDoc} Forwards the event to all registered listeners. */
  @Override
  public void onTaskComplete(ImportTaskResult taskResult) {
    for (ImportEventListener listener : listeners) {
      listener.onTaskComplete(taskResult);
    }
  }

  /** {@inheritDoc} Forwards the event to all registered listeners. */
  @Override
  public void onAllDataChunksCompleted() {
    for (ImportEventListener listener : listeners) {
      listener.onAllDataChunksCompleted();
    }
  }

  /**
   * Returns the current list of import data chunk status objects.
   *
   * @return an unmodifiable list of {@link ImportDataChunkStatus} objects
   */
  public List<ImportDataChunkStatus> getImportDataChunkStatusList() {
    return importDataChunkStatusList;
  }

  /**
   * Creates and returns a mapping of table column data types from the table metadata.
   *
   * @return a {@link TableColumnDataTypes} object containing the column data types for all tables
   */
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
