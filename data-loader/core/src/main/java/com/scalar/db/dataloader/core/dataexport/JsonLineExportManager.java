package com.scalar.db.dataloader.core.dataexport;

import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.dataexport.producer.ProducerTaskFactory;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDbDao;
import java.io.IOException;
import java.io.Writer;

/**
 * Export manager implementation which manages the export task that exports data in JSONLines format
 */
public class JsonLineExportManager extends ExportManager {

  /**
   * Constructs a {@code JsonLineExportManager} for exporting data using a {@link
   * DistributedTransactionManager}.
   *
   * <p>This constructor is used when exporting data in transactional mode, allowing data to be read
   * from ScalarDB within a distributed transaction context and exported in JSON Lines format.
   *
   * @param manager the {@link DistributedTransactionManager} used to read data in transactional
   *     mode
   * @param dao the {@link ScalarDbDao} used to interact with ScalarDB for export operations
   * @param producerTaskFactory the {@link ProducerTaskFactory} used to create producer tasks for
   *     exporting data
   */
  public JsonLineExportManager(
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
      throws IOException {}

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
}
