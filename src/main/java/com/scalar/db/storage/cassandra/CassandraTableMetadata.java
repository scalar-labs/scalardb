package com.scalar.db.storage.cassandra;

import com.datastax.driver.core.ClusteringOrder;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.IndexMetadata;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.Scan;
import com.scalar.db.exception.storage.UnsupportedTypeException;
import com.scalar.db.storage.common.metadata.DataType;
import com.scalar.db.storage.common.metadata.TableMetadata;
import com.scalar.db.storage.common.util.ImmutableLinkedHashSet;

import javax.annotation.concurrent.ThreadSafe;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@ThreadSafe
public class CassandraTableMetadata implements TableMetadata {
  private final LinkedHashSet<String> partitionKeyNames;
  private final LinkedHashSet<String> clusteringColumnNames;
  private final Set<String> indexNames;
  private final Map<String, DataType> columnDataTypes;
  private final Map<String, Scan.Ordering.Order> clusteringOrder;

  public CassandraTableMetadata(com.datastax.driver.core.TableMetadata tableMetadata) {
    this.partitionKeyNames =
        new ImmutableLinkedHashSet<>(
            tableMetadata.getPartitionKey().stream()
                .map(ColumnMetadata::getName)
                .collect(Collectors.toList()));
    this.clusteringColumnNames =
        new ImmutableLinkedHashSet<>(
            tableMetadata.getClusteringColumns().stream()
                .map(ColumnMetadata::getName)
                .collect(Collectors.toList()));
    indexNames =
        ImmutableSet.copyOf(
            tableMetadata.getIndexes().stream()
                .map(IndexMetadata::getTarget)
                .collect(Collectors.toSet()));
    columnDataTypes =
        ImmutableMap.copyOf(
            tableMetadata.getColumns().stream()
                .collect(
                    Collectors.toMap(
                        ColumnMetadata::getName, c -> convertDataType(c.getType().getName()))));

    Map<String, Scan.Ordering.Order> tmp = new HashMap<>();
    int i = 0;
    for (ColumnMetadata clusteringColumn : tableMetadata.getClusteringColumns()) {
      tmp.put(
          clusteringColumn.getName(), convertOrder(tableMetadata.getClusteringOrder().get(i++)));
    }
    clusteringOrder = ImmutableMap.copyOf(tmp);
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

  @Override
  public Set<String> getColumnNames() {
    return columnDataTypes.keySet();
  }

  @Override
  public DataType getColumnDataType(String columnName) {
    return columnDataTypes.get(columnName);
  }

  @Override
  public Scan.Ordering.Order getClusteringOrder(String clusteringKeyName) {
    return clusteringOrder.get(clusteringKeyName);
  }
}
