package com.scalar.db.storage.cosmos;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.implementation.NotFoundException;
import com.azure.cosmos.models.PartitionKey;
import com.scalar.db.api.Operation;
import com.scalar.db.exception.storage.StorageRuntimeException;
import com.scalar.db.storage.common.metadata.TableMetadataManager;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A manager to read and cache {@link CosmosTableMetadata} to know the type of each column
 *
 * @author Yuji Ito
 */
@ThreadSafe
public class CosmosTableMetadataManager implements TableMetadataManager {
  private final CosmosContainer container;
  private final Map<String, CosmosTableMetadata> tableMetadataMap;

  public CosmosTableMetadataManager(CosmosContainer container) {
    this.container = container;
    this.tableMetadataMap = new ConcurrentHashMap<>();
  }

  @Override
  public CosmosTableMetadata getTableMetadata(Operation operation) {
    if (!operation.forNamespace().isPresent() || !operation.forTable().isPresent()) {
      throw new IllegalArgumentException("operation has no target namespace and table name");
    }

    String fullName = operation.forFullTableName().get();
    if (!tableMetadataMap.containsKey(fullName)) {
      CosmosTableMetadata cosmosTableMetadata = readMetadata(fullName);
      if (cosmosTableMetadata == null) {
        return null;
      }
      tableMetadataMap.put(fullName, cosmosTableMetadata);
    }

    return tableMetadataMap.get(fullName);
  }

  private CosmosTableMetadata readMetadata(String fullName) {
    try {
      return container
          .readItem(fullName, new PartitionKey(fullName), CosmosTableMetadata.class)
          .getItem();
    } catch (NotFoundException e) {
      // The specified table is not found
      return null;
    } catch (CosmosException e) {
      throw new StorageRuntimeException("Failed to read the table metadata", e);
    }
  }
}
