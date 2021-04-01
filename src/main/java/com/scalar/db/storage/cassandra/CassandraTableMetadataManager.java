package com.scalar.db.storage.cassandra;

import com.datastax.driver.core.TableMetadata;
import com.scalar.db.api.Operation;
import com.scalar.db.exception.storage.StorageRuntimeException;
import com.scalar.db.storage.common.metadata.TableMetadataManager;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CassandraTableMetadataManager implements TableMetadataManager {

  private final Map<String, CassandraTableMetadata> tableMetadataMap;
  private final ClusterManager clusterManager;

  public CassandraTableMetadataManager(ClusterManager clusterManager) {
    this.clusterManager = clusterManager;
    tableMetadataMap = new ConcurrentHashMap<>();
  }

  @Override
  public CassandraTableMetadata getTableMetadata(Operation operation) {
    if (!operation.forNamespace().isPresent() || !operation.forTable().isPresent()) {
      throw new IllegalArgumentException("operation has no target namespace and table name");
    }

    String fullName = operation.forFullTableName().get();
    if (!tableMetadataMap.containsKey(fullName)) {
      try {
        TableMetadata metadata =
            clusterManager.getMetadata(
                operation.forFullNamespace().get(), operation.forTable().get());
        tableMetadataMap.put(fullName, new CassandraTableMetadata(metadata));
      } catch (StorageRuntimeException e) {
        // The specified table is not found
        return null;
      }
    }

    return tableMetadataMap.get(fullName);
  }
}
