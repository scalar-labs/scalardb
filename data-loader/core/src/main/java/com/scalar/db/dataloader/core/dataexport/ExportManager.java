package com.scalar.db.dataloader.core.dataexport;

import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionManagerCrudOperable;
import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.dataloader.core.ScalarDbMode;
import com.scalar.db.dataloader.core.dataexport.producer.ProducerTask;
import com.scalar.db.dataloader.core.dataexport.producer.ProducerTaskFactory;
import com.scalar.db.dataloader.core.dataexport.validation.ExportOptionsValidationException;
import com.scalar.db.dataloader.core.dataexport.validation.ExportOptionsValidator;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDbDao;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDbDaoException;
import com.scalar.db.dataloader.core.util.TableMetadataUtil;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.DataType;
import com.scalar.db.transaction.singlecrudoperation.SingleCrudOperationTransactionManager;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Export manager class which manages the export task */
@SuppressWarnings({"SameNameButDifferent", "FutureReturnValueIgnored"})
@RequiredArgsConstructor
public abstract class ExportManager {
  private static final Logger logger = LoggerFactory.getLogger(ExportManager.class);

  private final SingleCrudOperationTransactionManager singleCrudOperationTransactionManager;
  private final DistributedTransactionManager distributedTransactionManager;
  private final ScalarDbDao dao;
  private final ProducerTaskFactory producerTaskFactory;
  private final Object lock = new Object();

  /**
   * Constructs an {@code ExportManager} that uses a {@link SingleCrudOperationTransactionManager}
   * instance for non-transactional data export operations.
   *
   * @param singleCrudOperationTransactionManager the {@link SingleCrudOperationTransactionManager}
   *     used to execute single CRUD operations for reading data during export
   * @param dao the {@link ScalarDbDao} used to perform data operations
   * @param producerTaskFactory the factory for creating producer tasks to format the exported data
   */
  public ExportManager(
      SingleCrudOperationTransactionManager singleCrudOperationTransactionManager,
      ScalarDbDao dao,
      ProducerTaskFactory producerTaskFactory) {
    this.singleCrudOperationTransactionManager = singleCrudOperationTransactionManager;
    this.distributedTransactionManager = null;
    this.dao = dao;
    this.producerTaskFactory = producerTaskFactory;
  }

  /**
   * Constructs an {@code ExportManager} that uses a {@link DistributedTransactionManager} instance
   * for transactional data export operations.
   *
   * @param distributedTransactionManager the {@link DistributedTransactionManager} used to read
   *     data with transactional guarantees
   * @param dao the {@link ScalarDbDao} used to perform data operations
   * @param producerTaskFactory the factory for creating producer tasks to format the exported data
   */
  public ExportManager(
      DistributedTransactionManager distributedTransactionManager,
      ScalarDbDao dao,
      ProducerTaskFactory producerTaskFactory) {
    this.singleCrudOperationTransactionManager = null;
    this.distributedTransactionManager = distributedTransactionManager;
    this.dao = dao;
    this.producerTaskFactory = producerTaskFactory;
  }
  /**
   * Create and add header part for the export file
   *
   * @param exportOptions Export options for the data export
   * @param tableMetadata Metadata of the table to export
   * @param writer File writer object
   * @throws IOException If any IO exception occurs
   */
  abstract void processHeader(
      ExportOptions exportOptions, TableMetadata tableMetadata, Writer writer) throws IOException;

  /**
   * Create and add footer part for the export file
   *
   * @param exportOptions Export options for the data export
   * @param tableMetadata Metadata of the table to export
   * @param writer File writer object
   * @throws IOException If any IO exception occurs
   */
  abstract void processFooter(
      ExportOptions exportOptions, TableMetadata tableMetadata, Writer writer) throws IOException;
  /**
   * Starts the export process
   *
   * @param exportOptions Export options
   * @param tableMetadata Metadata for a single ScalarDB table
   * @param writer Writer to write the exported data
   * @return export report object containing data such as total exported row count
   */
  public ExportReport startExport(
      ExportOptions exportOptions, TableMetadata tableMetadata, Writer writer) {
    ExportReport exportReport = new ExportReport();
    ExecutorService executorService = null;

    try {
      validateExportOptions(exportOptions, tableMetadata);
      handleTransactionMetadata(exportOptions, tableMetadata);

      try (BufferedWriter bufferedWriter = new BufferedWriter(writer)) {
        processHeader(exportOptions, tableMetadata, bufferedWriter);

        int threadCount =
            exportOptions.getMaxThreadCount() > 0
                ? exportOptions.getMaxThreadCount()
                : Runtime.getRuntime().availableProcessors();
        executorService = Executors.newFixedThreadPool(threadCount);

        AtomicBoolean isFirstBatch = new AtomicBoolean(true);
        Map<String, DataType> dataTypeByColumnName = tableMetadata.getColumnDataTypes();

        if (exportOptions.getScalarDbMode() == ScalarDbMode.STORAGE) {
          try (TransactionManagerCrudOperable.Scanner scanner =
              createScannerWithStorage(exportOptions, dao, singleCrudOperationTransactionManager)) {
            submitTasks(
                scanner.iterator(),
                executorService,
                exportOptions,
                tableMetadata,
                dataTypeByColumnName,
                bufferedWriter,
                isFirstBatch,
                exportReport);
          }
        } else if (exportOptions.getScalarDbMode() == ScalarDbMode.TRANSACTION
            && distributedTransactionManager != null) {

          try (TransactionManagerCrudOperable.Scanner scanner =
              createScannerWithTransaction(exportOptions, dao, distributedTransactionManager)) {
            submitTasks(
                scanner.iterator(),
                executorService,
                exportOptions,
                tableMetadata,
                dataTypeByColumnName,
                bufferedWriter,
                isFirstBatch,
                exportReport);
          }
        }

        shutdownExecutor(executorService);
        processFooter(exportOptions, tableMetadata, bufferedWriter);
      } catch (TransactionException e) {
        throw new RuntimeException(e);
      }

    } catch (ExportOptionsValidationException
        | IOException
        | ScalarDbDaoException
        | InterruptedException e) {
      logger.error("Error during export: {}", e.getMessage());
    } finally {
      if (executorService != null && !executorService.isShutdown()) {
        executorService.shutdownNow();
      }
      closeResources();
    }

    return exportReport;
  }

  /**
   * Submits asynchronous tasks for processing chunks of data to the given executor service.
   *
   * <p>This method reads data from the provided {@code iterator} in chunks (based on the configured
   * chunk size) and submits each chunk as a separate task for processing. Each task invokes {@code
   * processDataChunk()} to write the data to the output format.
   *
   * <p>Any exceptions thrown during chunk processing are logged but do not halt the submission of
   * other tasks.
   *
   * @param iterator the iterator over database results
   * @param executorService the executor service to run the processing tasks
   * @param exportOptions configuration for export operation
   * @param tableMetadata metadata for the table being exported
   * @param dataTypeByColumnName mapping of column names to their data types
   * @param writer the writer to which export output is written
   * @param isFirstBatch an atomic flag used to track if the current chunk is the first one (used
   *     for formatting)
   * @param exportReport the report object that accumulates export statistics
   */
  private void submitTasks(
      Iterator<Result> iterator,
      ExecutorService executorService,
      ExportOptions exportOptions,
      TableMetadata tableMetadata,
      Map<String, DataType> dataTypeByColumnName,
      BufferedWriter writer,
      AtomicBoolean isFirstBatch,
      ExportReport exportReport) {
    while (iterator.hasNext()) {
      List<Result> chunk = fetchDataChunk(iterator, exportOptions.getDataChunkSize());
      executorService.submit(
          () -> {
            try {
              processDataChunk(
                  exportOptions,
                  tableMetadata,
                  dataTypeByColumnName,
                  chunk,
                  writer,
                  exportOptions.getOutputFileFormat() == FileFormat.JSON,
                  isFirstBatch,
                  exportReport);
            } catch (Exception e) {
              logger.error("Error processing data chunk", e);
            }
          });
    }
  }

  /**
   * Shuts down the given executor service gracefully, waiting for tasks to complete.
   *
   * <p>This method initiates an orderly shutdown where previously submitted tasks are executed, but
   * no new tasks will be accepted. It then waits for all tasks to finish within a specified
   * timeout. If the tasks do not complete in time, a warning is logged.
   *
   * @param executorService the ExecutorService to shut down
   * @throws InterruptedException if the current thread is interrupted while waiting
   */
  private void shutdownExecutor(ExecutorService executorService) throws InterruptedException {
    executorService.shutdown();
    if (!executorService.awaitTermination(60, TimeUnit.MINUTES)) {
      logger.warn("Timeout while waiting for export tasks to finish.");
    } else {
      logger.info("All export tasks completed.");
    }
  }

  /**
   * To process result data chunk
   *
   * @param exportOptions export options
   * @param tableMetadata metadata of the table
   * @param dataTypeByColumnName map of columns and their data types
   * @param dataChunk a list with result data
   * @param bufferedWriter writer object
   * @param isJson if data format is json or not
   * @param isFirstBatch is the data going to be process is the first batch or not
   * @param exportReport export report which will be updated once the data chunk is processed
   */
  private void processDataChunk(
      ExportOptions exportOptions,
      TableMetadata tableMetadata,
      Map<String, DataType> dataTypeByColumnName,
      List<Result> dataChunk,
      BufferedWriter bufferedWriter,
      boolean isJson,
      AtomicBoolean isFirstBatch,
      ExportReport exportReport) {
    ProducerTask producerTask =
        producerTaskFactory.createProducerTask(
            exportOptions.getOutputFileFormat(),
            exportOptions.getProjectionColumns(),
            tableMetadata,
            dataTypeByColumnName);
    String dataChunkContent = producerTask.process(dataChunk);

    try {
      synchronized (lock) {
        if (isJson && !isFirstBatch.getAndSet(false)) {
          bufferedWriter.write(",");
        }
        bufferedWriter.write(dataChunkContent);
        exportReport.updateExportedRowCount(dataChunk.size());
      }
    } catch (IOException e) {
      logger.error("Error while writing data chunk: {}", e.getMessage());
    }
  }

  /**
   * To split result into batches
   *
   * @param iterator iterator which parse results
   * @param batchSize size of batch
   * @return a list of results split to batches
   */
  private List<Result> fetchDataChunk(Iterator<Result> iterator, int batchSize) {
    List<Result> batch = new ArrayList<>();
    int count = 0;
    while (iterator.hasNext() && count < batchSize) {
      batch.add(iterator.next());
      count++;
    }
    return batch;
  }

  /**
   * * To validate export options
   *
   * @param exportOptions export options
   * @param tableMetadata metadata of the table
   * @throws ExportOptionsValidationException thrown if any of the export option validation fails
   */
  private void validateExportOptions(ExportOptions exportOptions, TableMetadata tableMetadata)
      throws ExportOptionsValidationException {
    ExportOptionsValidator.validate(exportOptions, tableMetadata);
  }

  /**
   * To update projection columns of export options if include metadata options is enabled
   *
   * @param exportOptions export options
   * @param tableMetadata metadata of the table
   */
  private void handleTransactionMetadata(ExportOptions exportOptions, TableMetadata tableMetadata) {
    if (exportOptions.isIncludeTransactionMetadata()
        && !exportOptions.getProjectionColumns().isEmpty()) {
      List<String> projectionMetadata =
          TableMetadataUtil.populateProjectionsWithMetadata(
              tableMetadata, exportOptions.getProjectionColumns());
      exportOptions.setProjectionColumns(projectionMetadata);
    }
  }

  /**
   * Creates a ScalarDB {@link TransactionManagerCrudOperable.Scanner} using the {@link
   * SingleCrudOperationTransactionManager} interface based on the scan configuration provided in
   * {@link ExportOptions}.
   *
   * <p>This method constructs a scanner for reading data directly from ScalarDB using single CRUD
   * operations managed by the given transaction manager.
   *
   * <p>If no partition key is specified in the {@code exportOptions}, a full table scan is
   * performed. Otherwise, a partition-specific scan is executed using the provided partition key,
   * optional scan range, sort orders, and projection columns.
   *
   * @param exportOptions the {@link ExportOptions} containing configuration for the export
   *     operation, including namespace, table name, projection columns, limit, scan range,
   *     partition key, and sort orders
   * @param dao the {@link ScalarDbDao} used to construct the scan operation for the specified table
   * @param manager the {@link SingleCrudOperationTransactionManager} used to execute the scan
   *     operations at the single CRUD level
   * @return a {@link TransactionManagerCrudOperable.Scanner} instance for reading data from
   *     ScalarDB using the provided transaction manager
   * @throws ScalarDbDaoException if an error occurs while creating the scanner or performing the
   *     scan setup
   */
  private TransactionManagerCrudOperable.Scanner createScannerWithStorage(
      ExportOptions exportOptions, ScalarDbDao dao, SingleCrudOperationTransactionManager manager)
      throws ScalarDbDaoException {
    boolean isScanAll = exportOptions.getScanPartitionKey() == null;
    if (isScanAll) {
      return dao.createScanner(
          exportOptions.getNamespace(),
          exportOptions.getTableName(),
          exportOptions.getProjectionColumns(),
          exportOptions.getLimit(),
          manager);
    } else {
      return dao.createScanner(
          exportOptions.getNamespace(),
          exportOptions.getTableName(),
          exportOptions.getScanPartitionKey(),
          exportOptions.getScanRange(),
          exportOptions.getSortOrders(),
          exportOptions.getProjectionColumns(),
          exportOptions.getLimit(),
          manager);
    }
  }

  /**
   * Creates a {@link TransactionManagerCrudOperable.Scanner} instance using the given {@link
   * ExportOptions}, {@link ScalarDbDao}, and {@link DistributedTransactionManager}.
   *
   * <p>If {@code scanPartitionKey} is not specified in {@code exportOptions}, a full table scan is
   * performed using the specified projection columns and limit. Otherwise, the scan is executed
   * with the specified partition key, range, sort orders, projection columns, and limit.
   *
   * @param exportOptions the export options containing scan configuration such as namespace, table
   *     name, partition key, projection columns, limit, range, and sort order
   * @param dao the ScalarDB DAO used to create the scanner
   * @param distributedTransactionManager the transaction manager to use for the scan operation
   * @return a {@link TransactionManagerCrudOperable.Scanner} for retrieving rows in transaction
   *     mode
   * @throws ScalarDbDaoException if an error occurs while creating the scanner
   * @throws TransactionException if a transaction-related error occurs during scanner creation
   */
  private TransactionManagerCrudOperable.Scanner createScannerWithTransaction(
      ExportOptions exportOptions,
      ScalarDbDao dao,
      DistributedTransactionManager distributedTransactionManager)
      throws ScalarDbDaoException, TransactionException {

    boolean isScanAll = exportOptions.getScanPartitionKey() == null;

    TransactionManagerCrudOperable.Scanner scanner;
    if (isScanAll) {
      scanner =
          dao.createScanner(
              exportOptions.getNamespace(),
              exportOptions.getTableName(),
              exportOptions.getProjectionColumns(),
              exportOptions.getLimit(),
              distributedTransactionManager);
    } else {
      scanner =
          dao.createScanner(
              exportOptions.getNamespace(),
              exportOptions.getTableName(),
              exportOptions.getScanPartitionKey(),
              exportOptions.getScanRange(),
              exportOptions.getSortOrders(),
              exportOptions.getProjectionColumns(),
              exportOptions.getLimit(),
              distributedTransactionManager);
    }

    return scanner;
  }

  /** Close resources properly once the process is completed */
  public void closeResources() {
    try {
      if (singleCrudOperationTransactionManager != null) {
        singleCrudOperationTransactionManager.close();
      } else if (distributedTransactionManager != null) {
        distributedTransactionManager.close();
      }
    } catch (Throwable e) {
      throw new RuntimeException("Failed to close the resource", e);
    }
  }
}
