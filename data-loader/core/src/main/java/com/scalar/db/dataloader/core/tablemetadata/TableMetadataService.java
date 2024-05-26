package com.scalar.db.dataloader.core.tablemetadata;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.util.TableMetadataUtils;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.service.StorageFactory;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * A service class that provides methods to get TableMetadata for a given namespace and table name.
 */
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

  /**
   * Returns the TableMetadata for the given list of TableMetadataRequest.
   *
   * @param requests List of TableMetadataRequest
   * @return Map of TableMetadata
   * @throws TableMetadataException if the namespace or table is missing
   */
  public Map<String, TableMetadata> getTableMetadata(Collection<TableMetadataRequest> requests)
      throws TableMetadataException {
    Map<String, TableMetadata> metadataMap = new HashMap<>();

    for (TableMetadataRequest request : requests) {
      String namespace = request.getNamespace();
      String tableName = request.getTableName();
      TableMetadata tableMetadata = getTableMetadata(namespace, tableName);
      String key = TableMetadataUtils.getTableLookupKey(namespace, tableName);
      metadataMap.put(key, tableMetadata);
    }

    return metadataMap;
  }
}
