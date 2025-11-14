package com.scalar.db.dataloader.core.dataexport;

import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.dataexport.producer.ProducerTaskFactory;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDbDao;
import com.scalar.db.dataloader.core.util.CsvUtil;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils;
import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import java.util.List;

/** Export manager implementation which manages the export task that exports data in CSV format */
public class CsvExportManager extends ExportManager {

  /**
   * Constructs a {@code CsvExportManager} for exporting data using a {@link
   * DistributedTransactionManager}.
   *
   * <p>This constructor is used when exporting data in transactional mode, allowing data to be read
   * from ScalarDB within a distributed transaction context.
   *
   * @param manager the {@link DistributedTransactionManager} used to read data in transactional
   *     mode
   * @param dao the {@link ScalarDbDao} used to interact with ScalarDB for export operations
   * @param producerTaskFactory the {@link ProducerTaskFactory} used to create producer tasks for
   *     exporting data
   */
  public CsvExportManager(
      DistributedTransactionManager manager,
      ScalarDbDao dao,
      ProducerTaskFactory producerTaskFactory) {
    super(manager, dao, producerTaskFactory);
  }

  /**
   * Create and add header part for the export file
   *
   * @param exportOptions Export options for the data export
   * @param tableMetadata Metadata of the table to export
   * @param writer File writer object
   * @throws IOException If any IO exception occurs
   */
  @Override
  void processHeader(ExportOptions exportOptions, TableMetadata tableMetadata, Writer writer)
      throws IOException {
    if (!exportOptions.isExcludeHeaderRow()) {
      String header = createCsvHeaderRow(exportOptions, tableMetadata);
      writer.append(header);
      writer.flush();
    }
  }

  /**
   * Create and add footer part for the export file
   *
   * @param exportOptions Export options for the data export
   * @param tableMetadata Metadata of the table to export
   * @param writer File writer object
   * @throws IOException If any IO exception occurs
   */
  @Override
  void processFooter(ExportOptions exportOptions, TableMetadata tableMetadata, Writer writer)
      throws IOException {}

  /**
   * To generate the header row of CSV export file
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
   * To ignore a column or not based on conditions such as if it is a metadata column or if it is
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
