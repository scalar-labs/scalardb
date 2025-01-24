package com.scalar.db.dataloader.core.tablemetadata;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.util.TableMetadataUtil;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;

/**
 * Service for retrieving {@link TableMetadata} from ScalarDB. Provides methods to fetch metadata
 * for individual tables or a collection of tables.
 */
@RequiredArgsConstructor
public class TableMetadataService {

  private final DistributedStorageAdmin storageAdmin;

  /**
   * Retrieves the {@link TableMetadata} for a specific namespace and table name.
   *
   * @param namespace The ScalarDB namespace.
   * @param tableName The name of the table within the specified namespace.
   * @return The {@link TableMetadata} object containing schema details of the specified table.
   * @throws TableMetadataException If the table or namespace does not exist, or if an error occurs
   *     while fetching the metadata.
   */
  public TableMetadata getTableMetadata(String namespace, String tableName)
      throws TableMetadataException {
    try {
      TableMetadata tableMetadata = storageAdmin.getTableMetadata(namespace, tableName);
      if (tableMetadata == null) {
        throw new TableMetadataException(
            CoreError.DATA_LOADER_MISSING_NAMESPACE_OR_TABLE.buildMessage(namespace, tableName));
      }
      return tableMetadata;
    } catch (ExecutionException e) {
      throw new TableMetadataException(
          CoreError.DATA_LOADER_TABLE_METADATA_RETRIEVAL_FAILED.buildMessage(e.getMessage()),
          e.getCause());
    }
  }

  /**
   * Retrieves the {@link TableMetadata} for a collection of table metadata requests.
   *
   * <p>Each request specifies a namespace and table name. The method consolidates the metadata into
   * a map keyed by a unique lookup key generated for each table.
   *
   * @param requests A collection of {@link TableMetadataRequest} objects specifying the tables to
   *     retrieve metadata for.
   * @return A map where the keys are unique lookup keys (namespace + table name) and the values are
   *     the corresponding {@link TableMetadata} objects.
   * @throws TableMetadataException If any of the requested tables or namespaces are missing, or if
   *     an error occurs while fetching the metadata.
   */
  public Map<String, TableMetadata> getTableMetadata(Collection<TableMetadataRequest> requests)
      throws TableMetadataException {
    Map<String, TableMetadata> metadataMap = new HashMap<>();

    for (TableMetadataRequest request : requests) {
      String namespace = request.getNamespace();
      String tableName = request.getTable();
      TableMetadata tableMetadata = getTableMetadata(namespace, tableName);
      String key = TableMetadataUtil.getTableLookupKey(namespace, tableName);
      metadataMap.put(key, tableMetadata);
    }

    return metadataMap;
  }
}
