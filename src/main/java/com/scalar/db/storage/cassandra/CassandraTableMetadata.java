package com.scalar.db.storage.cassandra;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.IndexMetadata;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.storage.ImmutableLinkedHashSet;
import com.scalar.db.storage.TableMetadata;

import javax.annotation.concurrent.ThreadSafe;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

@ThreadSafe
public class CassandraTableMetadata implements TableMetadata {
  private final LinkedHashSet<String> partitionKeyNames;
  private final LinkedHashSet<String> clusteringColumnNames;
  private final Set<String> indexNames;

  public CassandraTableMetadata(com.datastax.driver.core.TableMetadata tableMetadata) {
    this.partitionKeyNames =
        new ImmutableLinkedHashSet<>(
                tableMetadata.getPartitionKey().stream()
                    .map(ColumnMetadata::getName)
                    .collect(Collectors.toSet()))
            .immutable();
    this.clusteringColumnNames =
        new ImmutableLinkedHashSet<>(
                tableMetadata.getClusteringColumns().stream()
                    .map(ColumnMetadata::getName)
                    .collect(Collectors.toSet()))
            .immutable();
    this.indexNames =
        ImmutableSet.copyOf(
            tableMetadata.getIndexes().stream()
                .map(IndexMetadata::getTarget)
                .collect(Collectors.toSet()));
  }

  @Override
  public LinkedHashSet<String> getPartitionKeyNames() {
    return partitionKeyNames;
  }

  @Override
  public LinkedHashSet<String> getClusteringKeyNames() {
    return clusteringColumnNames;
  }

  @Override
  public Set<String> getSecondaryIndexNames() {
    return indexNames;
  }
}
