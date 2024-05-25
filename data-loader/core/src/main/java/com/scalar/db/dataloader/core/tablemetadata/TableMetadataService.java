package com.scalar.db.dataloader.core.tablemetadata;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.service.StorageFactory;

public class TableMetadataService {

  private final StorageFactory storageFactory;

  /**
   * Class constructor
   *
   * @param storageFactory a distributed storage admin
   */
  public TableMetadataService(StorageFactory storageFactory) {
    this.storageFactory = storageFactory;
  }

  /**
   * Returns the TableMetadata for the given namespace and table name.
   *
   * @param namespace ScalarDb namespace
   * @param tableName ScalarDb table name
   * @return TableMetadata
   * @throws TableMetadataException if the namespace or table is missing
   */
  public TableMetadata getTableMetadata(String namespace, String tableName)
      throws TableMetadataException {
    try {
      TableMetadata tableMetadata =
          storageFactory.getStorageAdmin().getTableMetadata(namespace, tableName);
      if (tableMetadata == null) {
        throw new TableMetadataException(
            CoreError.DATA_LOADER_MISSING_NAMESPACE_OR_TABLE.buildMessage(namespace, tableName));
      }
      return tableMetadata;
    } catch (ExecutionException e) {
      throw new TableMetadataException(
          CoreError.DATA_LOADER_MISSING_NAMESPACE_OR_TABLE.buildMessage(namespace, tableName));
    }
  }
}
