package com.scalar.db.dataloader.core.dataexport;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.dataloader.core.dataexport.producer.ProducerTask;
import com.scalar.db.dataloader.core.dataexport.producer.ProducerTaskFactory;
import com.scalar.db.dataloader.core.dataexport.validation.ExportOptionsValidationException;
import com.scalar.db.dataloader.core.dataexport.validation.ExportOptionsValidator;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDBDao;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDBDaoException;
import com.scalar.db.dataloader.core.util.CsvUtil;
import com.scalar.db.dataloader.core.util.TableMetadataUtil;
import com.scalar.db.io.DataType;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils;
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

@RequiredArgsConstructor
public class ExportManager {
  private static final Logger logger = LoggerFactory.getLogger(ExportManager.class);

  private final DistributedStorage storage;
  private final ScalarDBDao dao;
  private final ProducerTaskFactory producerTaskFactory;
  private final Object lock = new Object();

  /**
   * Starts the export process
   *
   * @param exportOptions Export options
   * @param tableMetadata Metadata for a single ScalarDB table
   * @param writer Writer to write the exported data
   */
  public ExportReport startExport(
      ExportOptions exportOptions, TableMetadata tableMetadata, Writer writer) {
    ExportReport exportReport = new ExportReport();
    try {
      validateExportOptions(exportOptions, tableMetadata);
      Map<String, DataType> dataTypeByColumnName = tableMetadata.getColumnDataTypes();
      handleTransactionMetadata(exportOptions, tableMetadata);

      if (exportOptions.getOutputFileFormat() == FileFormat.CSV
          && !exportOptions.isExcludeHeaderRow()) {
        writeCsvHeaderRow(exportOptions, tableMetadata, writer);
      }

      int maxThreadCount =
          exportOptions.getMaxThreadCount() == 0
              ? Runtime.getRuntime().availableProcessors()
              : exportOptions.getMaxThreadCount();
      ExecutorService executorService = Executors.newFixedThreadPool(maxThreadCount);

      BufferedWriter bufferedWriter = new BufferedWriter(writer);
      boolean isJson = exportOptions.getOutputFileFormat() == FileFormat.JSON;

      try (Scanner scanner = createScanner(exportOptions, dao, storage)) {
        if (isJson) {
          bufferedWriter.write("[");
        }

        Iterator<Result> iterator = scanner.iterator();
        AtomicBoolean isFirstBatch = new AtomicBoolean(true);

        while (iterator.hasNext()) {
          List<Result> dataChunk = fetchDataChunk(iterator, exportOptions.getDataChunkSize());
          executorService.submit(
              () ->
                  processDataChunk(
                      exportOptions,
                      tableMetadata,
                      dataTypeByColumnName,
                      dataChunk,
                      bufferedWriter,
                      isJson,
                      isFirstBatch,
                      exportReport));
        }
        executorService.shutdown();
        if (executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
          logger.info("All tasks completed");
        } else {
          logger.error("Timeout occurred while waiting for tasks to complete");
          // TODO: handle this
        }
        if (isJson) {
          bufferedWriter.write("]");
        }
        bufferedWriter.flush();
      } catch (InterruptedException | IOException e) {
        logger.error("Error during export: {}", e.getMessage());
      }
    } catch (ExportOptionsValidationException | IOException | ScalarDBDaoException e) {
      logger.error("Error during export: {}", e.getMessage());
    }
    return exportReport;
  }

  /**
   * * To process result data chunk
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
    String dataChunkContent = producerTask.process(dataChunk, exportReport);

    try {
      synchronized (lock) {
        if (isJson && !isFirstBatch.getAndSet(false)) {
          bufferedWriter.write(",");
        }
        bufferedWriter.write(dataChunkContent);
      }
    } catch (IOException e) {
      logger.error("Error while writing data chunk: {}", e.getMessage());
    }
  }

  /**
   * * To split result into batches
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
   * * To update projection columns of export options if include metadata options is enabled
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
   * * To create and write the header row to the CSV export file
   *
   * @param exportOptions export options
   * @param tableMetadata metadata of the table
   * @param writer writer object
   * @throws IOException throws if any exception occur in file operations
   */
  private void writeCsvHeaderRow(
      ExportOptions exportOptions, TableMetadata tableMetadata, Writer writer) throws IOException {
    String header = createCsvHeaderRow(exportOptions, tableMetadata);
    writer.append(header);
    writer.flush();
  }

  /**
   * * To create a scanner object
   *
   * @param exportOptions export options
   * @param dao scalardb dao object
   * @param storage distributed storage object
   * @return created scanner
   * @throws ScalarDBDaoException throws if any issue occurs in creating scanner object
   */
  private Scanner createScanner(
      ExportOptions exportOptions, ScalarDBDao dao, DistributedStorage storage)
      throws ScalarDBDaoException {
    boolean isScanAll = exportOptions.getScanPartitionKey() == null;
    if (isScanAll) {
      return dao.createScanner(
          exportOptions.getNamespace(),
          exportOptions.getTableName(),
          exportOptions.getProjectionColumns(),
          exportOptions.getLimit(),
          storage);
    } else {
      return dao.createScanner(
          exportOptions.getNamespace(),
          exportOptions.getTableName(),
          exportOptions.getScanPartitionKey(),
          exportOptions.getScanRange(),
          exportOptions.getSortOrders(),
          exportOptions.getProjectionColumns(),
          exportOptions.getLimit(),
          storage);
    }
  }

  /**
   * * To generate the header row of CSV export file
   *
   * @param exportOptions export options
   * @param tableMetadata metadata of the table
   * @return generated CSV header row
   */
  private String createCsvHeaderRow(ExportOptions exportOptions, TableMetadata tableMetadata) {
    StringBuilder headerRow = new StringBuilder();
    List<String> projections = exportOptions.getProjectionColumns();
    Iterator<String> iterator = tableMetadata.getColumnNames().iterator();
    while (iterator.hasNext()) {
      String columnName = iterator.next();
      if (shouldIgnoreColumn(
          exportOptions.isIncludeTransactionMetadata(), columnName, tableMetadata, projections)) {
        continue;
      }
      headerRow.append(columnName);
      if (iterator.hasNext()) {
        headerRow.append(exportOptions.getDelimiter());
      }
    }
    CsvUtil.removeTrailingDelimiter(headerRow, exportOptions.getDelimiter());
    headerRow.append("\n");
    return headerRow.toString();
  }

  /**
   * * To ignore a column or not based on conditions such as if it is a metadata column or if it is
   * not include in selected projections
   *
   * @param isIncludeTransactionMetadata to include transaction metadata or not
   * @param columnName column name
   * @param tableMetadata table metadata
   * @param projections selected columns for projection
   * @return ignore the column or not
   */
  private boolean shouldIgnoreColumn(
      boolean isIncludeTransactionMetadata,
      String columnName,
      TableMetadata tableMetadata,
      List<String> projections) {
    return (!isIncludeTransactionMetadata
            && ConsensusCommitUtils.isTransactionMetaColumn(columnName, tableMetadata))
        || (!projections.isEmpty() && !projections.contains(columnName));
  }
}
