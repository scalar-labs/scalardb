package com.scalar.db.storage.cosmos;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.PartitionKey;
import com.scalar.db.api.Operation;
import com.scalar.db.exception.storage.StorageRuntimeException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A manager to read and cache {@link CosmosTableMetadata} to know the type of each column
 *
 * @author Yuji Ito
 */
@ThreadSafe
public class TableMetadataManager {
  private final CosmosContainer container;
  private final Map<String, CosmosTableMetadata> tableMetadataMap;

  public TableMetadataManager(CosmosContainer container) {
    this.container = container;
    this.tableMetadataMap = new ConcurrentHashMap<>();
  }

  public CosmosTableMetadata getTableMetadata(Operation operation) {
    if (!operation.forNamespace().isPresent() || !operation.forTable().isPresent()) {
      throw new IllegalArgumentException("operation has no target namespace and table name");
    }

    String fullName = operation.forFullTableName().get();
    if (!tableMetadataMap.containsKey(fullName)) {
      tableMetadataMap.put(fullName, readMetadata(fullName));
    }

    return tableMetadataMap.get(fullName);
  }

  private CosmosTableMetadata readMetadata(String fullName) {
    try {
      return container
          .readItem(fullName, new PartitionKey(fullName), CosmosTableMetadata.class)
          .getItem();
    } catch (CosmosException e) {
      throw new StorageRuntimeException("Failed to read the table metadata", e);
    }
  }
}
