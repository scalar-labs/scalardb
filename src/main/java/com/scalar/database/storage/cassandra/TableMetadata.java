package com.scalar.database.storage.cassandra;

import com.datastax.driver.core.ColumnMetadata;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class TableMetadata {
  private final Set<String> partitionKeyNames;
  private final Set<String> clusteringColumnNames;

  public TableMetadata(com.datastax.driver.core.TableMetadata tableMetadata) {
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
