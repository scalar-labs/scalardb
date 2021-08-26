package com.scalar.db.storage.cassandra;

import static com.scalar.db.util.Utility.getFullNamespaceName;
import static com.scalar.db.util.Utility.getFullTableName;

import com.datastax.driver.core.ClusteringOrder;
import com.datastax.driver.core.KeyspaceMetadata;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ConnectionException;
import com.scalar.db.exception.storage.StorageRuntimeException;
import com.scalar.db.exception.storage.UnsupportedTypeException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.common.TableMetadataManager;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class CassandraTableMetadataManager implements TableMetadataManager {

  private final Map<String, TableMetadata> tableMetadataMap;
  private final ClusterManager clusterManager;
  private final Optional<String> keyspacePrefix;

  public CassandraTableMetadataManager(
      ClusterManager clusterManager, Optional<String> keyspacePrefix) {
    this.clusterManager = clusterManager;
    tableMetadataMap = new ConcurrentHashMap<>();
    this.keyspacePrefix = keyspacePrefix;
  }

  /**
   * Return the Scalar DB datatype value that is equivalent to {@link
   * com.datastax.driver.core.DataType}
   *
   * @return Scalar DB datatype that is equivalent {@link com.datastax.driver.core.DataType}
   */
  private DataType fromCassandraDataType(
      com.datastax.driver.core.DataType.Name cassandraDataTypeName) {
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
        throw new UnsupportedTypeException(
            String.format("%s is not yet supported", cassandraDataTypeName));
    }
  }

  @Override
  public TableMetadata getTableMetadata(Operation operation) {
    if (!operation.forNamespace().isPresent() || !operation.forTable().isPresent()) {
      throw new IllegalArgumentException("operation has no target namespace and table name");
    }
    return getTableMetadata(operation.forNamespace().get(), operation.forTable().get());
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) {
    String fullKeyspace = getFullNamespaceName(keyspacePrefix, namespace);
    String fullTableName = getFullTableName(keyspacePrefix, namespace, table);
    if (!tableMetadataMap.containsKey(fullTableName)) {
      try {
        com.datastax.driver.core.TableMetadata metadata =
            clusterManager.getMetadata(fullKeyspace, table);
        if (metadata == null) {
          return null;
        }
        tableMetadataMap.put(fullTableName, createTableMetadata(metadata));
      } catch (ConnectionException e) {
        throw new StorageRuntimeException("Failed to read the table metadata", e);
      }
    }

    return tableMetadataMap.get(fullTableName);
  }

  private TableMetadata createTableMetadata(com.datastax.driver.core.TableMetadata metadata) {
    TableMetadata.Builder builder = TableMetadata.newBuilder();
    metadata
        .getColumns()
        .forEach(c -> builder.addColumn(c.getName(), fromCassandraDataType(c.getType().getName())));
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
    String fullName = getFullTableName(keyspacePrefix, namespace, table);
    tableMetadataMap.remove(fullName);
  }

  @Override
  public void addTableMetadata(String namespace, String table, TableMetadata metadata) {
    // Table metadata can be retrieved from the ClusterManager directly once the table has been
    // inserted to Cassandra so we don't need to do anything here
  }

  @Override
  public Set<String> getTableNames(String namespace) {
    KeyspaceMetadata keyspace =
        clusterManager
            .getSession()
            .getCluster()
            .getMetadata()
            .getKeyspace(getFullNamespaceName(keyspacePrefix, namespace));
    if (keyspace == null) {
      return Collections.emptySet();
    }
    return keyspace.getTables().stream()
        .map(com.datastax.driver.core.TableMetadata::getName)
        .collect(Collectors.toSet());
  }
}
