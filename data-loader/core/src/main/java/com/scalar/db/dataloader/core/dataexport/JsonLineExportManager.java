package com.scalar.db.dataloader.core.dataexport;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.dataexport.producer.ProducerTaskFactory;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDbDao;
import java.io.IOException;
import java.io.Writer;

public class JsonLineExportManager extends ExportManager {
  /**
   * Constructs a {@code JsonLineExportManager} for exporting data using a {@link
   * DistributedStorage} instance.
   *
   * <p>This constructor is used when exporting data in non-transactional (storage) mode.
   *
   * @param distributedStorage the {@link DistributedStorage} used to read data directly from
   *     storage
   * @param dao the {@link ScalarDbDao} used to interact with ScalarDB for exporting data
   * @param producerTaskFactory the factory used to create producer tasks for generating
   *     CSV-formatted output
   */
  public JsonLineExportManager(
      DistributedStorage distributedStorage,
      ScalarDbDao dao,
      ProducerTaskFactory producerTaskFactory) {
    super(distributedStorage, dao, producerTaskFactory);
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
