package com.scalar.db.storage.cassandra;

import com.datastax.driver.core.ClusteringOrder;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TableMetadata.Builder;
import com.scalar.db.exception.storage.StorageRuntimeException;
import com.scalar.db.exception.storage.UnsupportedTypeException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.common.TableMetadataManager;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CassandraTableMetadataManager implements TableMetadataManager {

  private final Map<String, TableMetadata> tableMetadataMap;
  private final ClusterManager clusterManager;

  public CassandraTableMetadataManager(ClusterManager clusterManager) {
    this.clusterManager = clusterManager;
    tableMetadataMap = new ConcurrentHashMap<>();
  }

  @Override
  public TableMetadata getTableMetadata(Operation operation) {
    if (!operation.forNamespace().isPresent() || !operation.forTable().isPresent()) {
      throw new IllegalArgumentException("operation has no target namespace and table name");
    }

    String fullName = operation.forFullTableName().get();
    if (!tableMetadataMap.containsKey(fullName)) {
      try {
        com.datastax.driver.core.TableMetadata metadata =
            clusterManager.getMetadata(
                operation.forFullNamespace().get(), operation.forTable().get());
        tableMetadataMap.put(fullName, createTableMetadata(metadata));
      } catch (StorageRuntimeException e) {
        // The specified table is not found
        return null;
      }
    }

    return tableMetadataMap.get(fullName);
  }

  private TableMetadata createTableMetadata(com.datastax.driver.core.TableMetadata metadata) {
    Builder builder = TableMetadata.newBuilder();
    metadata
        .getColumns()
        .forEach(c -> builder.addColumn(c.getName(), convertDataType(c.getType().getName())));
    metadata.getPartitionKey().forEach(c -> builder.addPartitionKey(c.getName()));
    for (int i = 0; i < metadata.getClusteringColumns().size(); i++) {
      String clusteringColumnName = metadata.getClusteringColumns().get(i).getName();
      ClusteringOrder clusteringOrder = metadata.getClusteringOrder().get(i);
      builder.addClusteringKey(clusteringColumnName, convertOrder(clusteringOrder));
    }
    metadata.getIndexes().forEach(i -> builder.addSecondaryIndex(i.getTarget()));
    return builder.build();
  }

  private DataType convertDataType(com.datastax.driver.core.DataType.Name cassandraDataTypeName) {
    switch (cassandraDataTypeName) {
      case INT:
        return DataType.INT;
      case BIGINT:
        return DataType.BIGINT;
      case FLOAT:
        return DataType.FLOAT;
      case DOUBLE:
        return DataType.DOUBLE;
      case TEXT:
        return DataType.TEXT;
      case BOOLEAN:
        return DataType.BOOLEAN;
      case BLOB:
        return DataType.BLOB;
      default:
        throw new UnsupportedTypeException(cassandraDataTypeName.toString());
    }
  }

  private Scan.Ordering.Order convertOrder(ClusteringOrder clusteringOrder) {
    switch (clusteringOrder) {
      case ASC:
        return Scan.Ordering.Order.ASC;
      case DESC:
        return Scan.Ordering.Order.DESC;
      default:
        throw new AssertionError();
    }
  }
}
