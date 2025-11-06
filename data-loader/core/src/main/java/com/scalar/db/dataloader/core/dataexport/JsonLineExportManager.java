package com.scalar.db.dataloader.core.dataexport;

import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.dataexport.producer.ProducerTaskFactory;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDbDao;
import com.scalar.db.transaction.singlecrudoperation.SingleCrudOperationTransactionManager;
import java.io.IOException;
import java.io.Writer;

/**
 * Export manager implementation which manages the export task that exports data in JSONLines format
 */
public class JsonLineExportManager extends ExportManager {
  /**
   * Constructs a {@code CsvExportManager} for exporting data using a {@link
   * SingleCrudOperationTransactionManager}.
   *
   * <p>This constructor is used when exporting data in non-transactional (single CRUD) mode, where
   * data is read directly through the {@link SingleCrudOperationTransactionManager} without
   * distributed transactions.
   *
   * @param singleCrudOperationTransactionManager the {@link SingleCrudOperationTransactionManager}
   *     used to execute single CRUD read operations during the export process
   * @param dao the {@link ScalarDbDao} used to interact with ScalarDB for exporting data
   * @param producerTaskFactory the factory used to create producer tasks for generating
   *     CSV-formatted output
   */
  public JsonLineExportManager(
      SingleCrudOperationTransactionManager singleCrudOperationTransactionManager,
      ScalarDbDao dao,
      ProducerTaskFactory producerTaskFactory) {
    super(singleCrudOperationTransactionManager, dao, producerTaskFactory);
  }

  /**
   * Constructs a {@code JsonLineExportManager} for exporting data using a {@link
   * DistributedTransactionManager}.
   *
   * @param distributedTransactionManager the transaction manager used to read data with
   *     transactional guarantees
   * @param dao the {@link ScalarDbDao} used to interact with ScalarDB for exporting data
   * @param producerTaskFactory the factory used to create producer tasks for generating
   *     CSV-formatted output
   */
  public JsonLineExportManager(
      DistributedTransactionManager distributedTransactionManager,
      ScalarDbDao dao,
      ProducerTaskFactory producerTaskFactory) {
    super(distributedTransactionManager, dao, producerTaskFactory);
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
