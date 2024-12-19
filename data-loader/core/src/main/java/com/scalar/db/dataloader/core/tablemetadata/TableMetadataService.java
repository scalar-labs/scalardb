package com.scalar.db.dataloader.core.tablemetadata;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.util.TableMetadataUtil;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TableMetadataService {
  private static final String ERROR_MISSING_NAMESPACE_OR_TABLE =
      "Missing namespace or table: %s, %s";

  private final DistributedStorageAdmin storageAdmin;

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
      String key = TableMetadataUtil.getTableLookupKey(namespace, tableName);
      metadataMap.put(key, tableMetadata);
    }

    return metadataMap;
  }
}
