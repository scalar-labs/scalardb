package com.scalar.db.dataloader.core.dataexport;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.dataexport.producer.ProducerTaskFactory;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDbDao;
import java.io.IOException;
import java.io.Writer;

/** Export manager implementation which manages the export task that exports data in JSON format */
public class JsonExportManager extends ExportManager {

  /**
   * Constructs a {@code JsonExportManager} with the specified {@link DistributedTransactionManager}, {@link
   * ScalarDbDao}, and {@link ProducerTaskFactory}.
   *
   * @param manager the {@code DistributedTransactionManager} instance used to read data from the database
   * @param dao the {@code ScalarDbDao} used to execute export-related database operations
   * @param producerTaskFactory the factory used to create producer tasks for exporting data
   */
  public JsonExportManager(
          DistributedTransactionManager manager, ScalarDbDao dao, ProducerTaskFactory producerTaskFactory) {
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
    writer.write("[");
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
      throws IOException {
    writer.write("]");
  }
}
