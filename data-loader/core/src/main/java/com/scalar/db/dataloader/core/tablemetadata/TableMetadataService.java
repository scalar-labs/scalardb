package com.scalar.db.dataloader.core.tablemetadata;

import static com.scalar.db.dataloader.core.constant.ErrorMessages.ERROR_MISSING_NAMESPACE_OR_TABLE;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;

public class TableMetadataService {

  private final DistributedStorageAdmin storageAdmin;

  /**
   * Class constructor
   *
   * @param storageAdmin a distributed storage admin
   */
  public TableMetadataService(DistributedStorageAdmin storageAdmin) {
    this.storageAdmin = storageAdmin;
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
      TableMetadata tableMetadata = storageAdmin.getTableMetadata(namespace, tableName);
      if (tableMetadata == null) {
        throw new TableMetadataException(
            String.format(ERROR_MISSING_NAMESPACE_OR_TABLE, namespace, tableName));
      }
      return tableMetadata;
    } catch (ExecutionException e) {
      throw new TableMetadataException(
          String.format(ERROR_MISSING_NAMESPACE_OR_TABLE, namespace, tableName), e.getCause());
    }
  }
}
