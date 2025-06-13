package com.scalar.db.dataloader.core.tablemetadata;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.util.TableMetadataUtil;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Abstract base class for retrieving {@link TableMetadata} from ScalarDB. Provides shared logic for
 * fetching metadata for a collection of tables. Subclasses must implement the specific logic for
 * fetching individual table metadata.
 */
public abstract class TableMetadataService {

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
    TableMetadata metadata = getTableMetadataInternal(namespace, tableName);
    if (metadata == null) {
      throw new TableMetadataException(
          CoreError.DATA_LOADER_MISSING_NAMESPACE_OR_TABLE.buildMessage(namespace, tableName));
    }
    return metadata;
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
      TableMetadata metadata = getTableMetadata(namespace, tableName);
      String key = TableMetadataUtil.getTableLookupKey(namespace, tableName);
      metadataMap.put(key, metadata);
    }
    return metadataMap;
  }

  /**
   * Abstract method for retrieving table metadata for a specific namespace and table. Subclasses
   * must implement this to define how the metadata is fetched.
   *
   * @param namespace The namespace of the table.
   * @param tableName The table name.
   * @return The {@link TableMetadata} object, or null if not found.
   * @throws TableMetadataException if an error occurs during metadata retrieval.
   */
  protected abstract TableMetadata getTableMetadataInternal(String namespace, String tableName)
      throws TableMetadataException;
}
