package com.scalar.db.dataloader.core.dataexport;

import com.scalar.db.api.DistributedStorage;
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
   * Constructs a {@code JsonLineExportManager} with the specified {@link DistributedStorage},
   * {@link ScalarDbDao}, and {@link ProducerTaskFactory}.
   *
   * @param storage the {@code DistributedStorage} instance used to read data from the database
   * @param dao the {@code ScalarDbDao} used to execute export-related database operations
   * @param producerTaskFactory the factory used to create producer tasks for exporting data
   */
  public JsonLineExportManager(
      DistributedStorage storage, ScalarDbDao dao, ProducerTaskFactory producerTaskFactory) {
    super(storage, dao, producerTaskFactory);
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
