package com.scalar.db.storage.cassandra;

import com.datastax.driver.core.ColumnMetadata;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.storage.TableMetadata;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class CassandraTableMetadata implements TableMetadata {
  private final Set<String> partitionKeyNames;
  private final Set<String> clusteringColumnNames;

  public CassandraTableMetadata(com.datastax.driver.core.TableMetadata tableMetadata) {
    this.partitionKeyNames =
        ImmutableSet.copyOf(
            tableMetadata.getPartitionKey().stream()
                .map(ColumnMetadata::getName)
                .collect(Collectors.toSet()));
    this.clusteringColumnNames =
        ImmutableSet.copyOf(
            tableMetadata.getClusteringColumns().stream()
                .map(ColumnMetadata::getName)
                .collect(Collectors.toSet()));
  }

  @Override
  public Set<String> getPartitionKeyNames() {
    return partitionKeyNames;
  }

  @Override
  public Set<String> getClusteringKeyNames() {
    return clusteringColumnNames;
  }
}
