package com.scalar.database.storage.cassandra;

import com.datastax.driver.core.ColumnMetadata;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class TableMetadata {
  private final ClusterManager manager;
  private final String keyspace;
  private final String tableName;
  private com.datastax.driver.core.TableMetadata tableMetadata;
  private Set<String> partitionKeyNames;
  private Set<String> clusteringColumnNames;

  public TableMetadata(ClusterManager manager, String keyspace, String tableName) {
    this.manager = manager;
    this.keyspace = keyspace;
    this.tableName = tableName;
    this.tableMetadata = manager.getMetadata(keyspace, tableName);
    this.partitionKeyNames =
        ImmutableSet.copyOf(
            tableMetadata
                .getPartitionKey()
                .stream()
                .map(ColumnMetadata::getName)
                .collect(Collectors.toSet()));
    this.clusteringColumnNames =
        ImmutableSet.copyOf(
            tableMetadata
                .getClusteringColumns()
                .stream()
                .map(ColumnMetadata::getName)
                .collect(Collectors.toSet()));
  }

  public Set<String> getPartitionKeyNames() {
    return partitionKeyNames;
  }

  public Set<String> getClusteringColumnNames() {
    return clusteringColumnNames;
  }
}
