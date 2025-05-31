package com.scalar.db.dataloader.core.dataimport;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.ScalarDbMode;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDbDao;
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
import java.util.concurrent.ConcurrentHashMap;
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
  private final ScalarDbMode scalarDbMode;
  private final DistributedStorage distributedStorage;
  private final DistributedTransactionManager distributedTransactionManager;
  private final ConcurrentHashMap<Integer, ImportDataChunkStatus> importDataChunkStatusMap =
      new ConcurrentHashMap<>();

  /**
   * Starts the import process using the configured parameters.
   *
   * <p>If the data chunk size in {@link ImportOptions} is set to 0, the entire file will be
   * processed as a single chunk. Otherwise, the file will be processed in chunks of the specified
   * size.
   *
   * @return a map of {@link ImportDataChunkStatus} objects containing the status of each processed
   *     chunk
   */
  public ConcurrentHashMap<Integer, ImportDataChunkStatus> startImport() {
    ImportProcessorParams params =
        ImportProcessorParams.builder()
            .scalarDbMode(scalarDbMode)
            .importOptions(importOptions)
            .tableMetadataByTableName(tableMetadata)
            .dao(new ScalarDbDao())
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
   * {@inheritDoc} Updates or adds the status of a data chunk in the status map. This method is
   * thread-safe.
   */
  @Override
  public void addOrUpdateDataChunkStatus(ImportDataChunkStatus status) {
    importDataChunkStatusMap.put(status.getDataChunkId(), status);
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
    Throwable firstException = null;

    for (ImportEventListener listener : listeners) {
      try {
        listener.onAllDataChunksCompleted();
      } catch (Throwable e) {
        if (firstException == null) {
          firstException = e;
        } else {
          firstException.addSuppressed(e);
        }
      }
    }

    try {
      closeResources();
    } catch (Throwable e) {
      if (firstException != null) {
        firstException.addSuppressed(e);
      } else {
        firstException = e;
      }
    }

    if (firstException != null) {
      throw new RuntimeException("Error during completion", firstException);
    }
  }

  /** Close resources properly once the process is completed */
  public void closeResources() {
    try {
      if (distributedStorage != null) {
        distributedStorage.close();
      } else if (distributedTransactionManager != null) {
        distributedTransactionManager.close();
      }
    } catch (Throwable e) {
      throw new RuntimeException("Failed to close the resource", e);
    }
  }

  /**
   * Returns the current map of import data chunk status objects.
   *
   * @return a map of {@link ImportDataChunkStatus} objects
   */
  public ConcurrentHashMap<Integer, ImportDataChunkStatus> getImportDataChunkStatus() {
    return importDataChunkStatusMap;
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
