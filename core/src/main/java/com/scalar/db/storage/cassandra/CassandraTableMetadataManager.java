package com.scalar.db.storage.cassandra;

import com.datastax.driver.core.ClusteringOrder;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ConnectionException;
import com.scalar.db.exception.storage.StorageRuntimeException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.common.TableMetadataManager;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class CassandraTableMetadataManager implements TableMetadataManager {

  private final Map<String, TableMetadata> tableMetadataMap;
  private final ClusterManager clusterManager;
  private final Optional<String> namespacePrefix;

  public CassandraTableMetadataManager(
      ClusterManager clusterManager, Optional<String> namespacePrefix) {
    this.clusterManager = clusterManager;
    tableMetadataMap = new ConcurrentHashMap<>();
    this.namespacePrefix = namespacePrefix;
  }

  @Override
  public TableMetadata getTableMetadata(Operation operation) {
    if (!operation.forNamespace().isPresent() || !operation.forTable().isPresent()) {
      throw new IllegalArgumentException("operation has no target namespace and table name");
    }
    return getTableMetadata(operation.forFullNamespace().get(), operation.forTable().get());
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) {
    // TODO replace by utility
    String fullNamespace = namespacePrefix.orElse("") + namespace;
    String fullName = fullNamespace + "." + table;
    if (!tableMetadataMap.containsKey(fullName)) {
      try {
        com.datastax.driver.core.TableMetadata metadata =
            clusterManager.getMetadata(fullNamespace, table);
        if (metadata == null) {
          return null;
        }
        tableMetadataMap.put(fullName, createTableMetadata(metadata));
      } catch (ConnectionException e) {
        throw new StorageRuntimeException("Failed to read the table metadata", e);
      }
    }

    return tableMetadataMap.get(fullName);
  }

  private TableMetadata createTableMetadata(com.datastax.driver.core.TableMetadata metadata) {
    TableMetadata.Builder builder = TableMetadata.newBuilder();
    metadata
        .getColumns()
        .forEach(
            c ->
                builder.addColumn(
                    c.getName(), DataType.fromCassandraDataType(c.getType().getName())));
    metadata.getPartitionKey().forEach(c -> builder.addPartitionKey(c.getName()));
    for (int i = 0; i < metadata.getClusteringColumns().size(); i++) {
      String clusteringColumnName = metadata.getClusteringColumns().get(i).getName();
      ClusteringOrder clusteringOrder = metadata.getClusteringOrder().get(i);
      builder.addClusteringKey(clusteringColumnName, convertOrder(clusteringOrder));
    }
    metadata.getIndexes().forEach(i -> builder.addSecondaryIndex(i.getTarget()));
    return builder.build();
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

  @Override
  public void deleteTableMetadata(String namespace, String table) {
    String fullName = namespacePrefix.orElse("") + namespace + "." + table;
    tableMetadataMap.remove(fullName);
  }

  @Override
  public void addTableMetadata(String namespace, String table, TableMetadata metadata) {
    // Table metadata can be retrieved from the ClusterManager directly once the table has been
    // inserted to Cassandra so we don't need to do anything here
  }
}
