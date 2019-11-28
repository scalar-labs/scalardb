package com.scalar.database.storage.cassandra4driver;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class TableMetadata {
  private final Set<String> partitionKeyNames;
  private final Set<String> clusteringColumnNames;

  public TableMetadata(
      com.datastax.oss.driver.api.core.metadata.schema.TableMetadata tableMetadata) {
    this.partitionKeyNames =
        ImmutableSet.copyOf(
            tableMetadata
                .getPartitionKey()
                .stream()
                .map(ColumnMetadata::getName)
                .map(CqlIdentifier::toString)
                .collect(Collectors.toSet()));
    this.clusteringColumnNames =
        ImmutableSet.copyOf(
            tableMetadata
                .getClusteringColumns()
                .keySet()
                .stream()
                .map(ColumnMetadata::getName)
                .map(CqlIdentifier::toString)
                .collect(Collectors.toSet()));
  }

  public Set<String> getPartitionKeyNames() {
    return partitionKeyNames;
  }

  public Set<String> getClusteringColumnNames() {
    return clusteringColumnNames;
  }
}
