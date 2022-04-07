package com.scalar.db.sql.metadata;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.Scan;
import com.scalar.db.sql.ClusteringOrder;
import com.scalar.db.sql.DataType;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.concurrent.Immutable;

@Immutable
public class TableMetadata {

  private final String namespaceName;
  private final String name;
  private final ImmutableList<ColumnMetadata> primaryKey;
  private final ImmutableList<ColumnMetadata> partitionKey;
  private final ImmutableSet<String> partitionKeyColumnNames;
  private final ImmutableMap<ColumnMetadata, ClusteringOrder> clusteringKey;
  private final ImmutableSet<String> clusteringKeyColumnNames;
  private final ImmutableMap<String, ColumnMetadata> columns;
  private final ImmutableMap<String, IndexMetadata> indexes;

  TableMetadata(String namespaceName, String name, com.scalar.db.api.TableMetadata tableMetadata) {
    this.namespaceName = Objects.requireNonNull(namespaceName);
    this.name = Objects.requireNonNull(name);

    partitionKey =
        ImmutableList.copyOf(
            tableMetadata.getPartitionKeyNames().stream()
                .map(
                    c ->
                        new ColumnMetadata(
                            namespaceName,
                            name,
                            c,
                            convertDataType(tableMetadata.getColumnDataType(c))))
                .collect(Collectors.toList()));

    partitionKeyColumnNames =
        ImmutableSet.copyOf(
            partitionKey.stream().map(ColumnMetadata::getName).collect(Collectors.toList()));

    clusteringKey =
        ImmutableMap.copyOf(
            tableMetadata.getClusteringKeyNames().stream()
                .map(
                    c ->
                        new ColumnMetadata(
                            namespaceName,
                            name,
                            c,
                            convertDataType(tableMetadata.getColumnDataType(c))))
                .collect(
                    Collectors.toMap(
                        Function.identity(),
                        c ->
                            convertClusteringOrder(
                                tableMetadata.getClusteringOrder(c.getName())))));

    clusteringKeyColumnNames =
        ImmutableSet.copyOf(
            clusteringKey.keySet().stream()
                .map(ColumnMetadata::getName)
                .collect(Collectors.toList()));

    primaryKey =
        ImmutableList.<ColumnMetadata>builder()
            .addAll(partitionKey)
            .addAll(clusteringKey.keySet())
            .build();

    columns =
        ImmutableMap.copyOf(
            tableMetadata.getColumnNames().stream()
                .map(
                    c ->
                        new ColumnMetadata(
                            namespaceName,
                            name,
                            c,
                            convertDataType(tableMetadata.getColumnDataType(c))))
                .collect(Collectors.toMap(ColumnMetadata::getName, Function.identity())));

    indexes =
        ImmutableMap.copyOf(
            tableMetadata.getSecondaryIndexNames().stream()
                .collect(
                    Collectors.toMap(
                        Function.identity(), c -> new IndexMetadata(namespaceName, name, c))));
  }

  private DataType convertDataType(com.scalar.db.io.DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return DataType.BOOLEAN;
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
      case BLOB:
        return DataType.BLOB;
      default:
        throw new AssertionError();
    }
  }

  private ClusteringOrder convertClusteringOrder(Scan.Ordering.Order order) {
    switch (order) {
      case ASC:
        return ClusteringOrder.ASC;
      case DESC:
        return ClusteringOrder.DESC;
      default:
        throw new AssertionError();
    }
  }

  public String getNamespaceName() {
    return namespaceName;
  }

  public String getName() {
    return name;
  }

  public List<ColumnMetadata> getPrimaryKey() {
    return primaryKey;
  }

  public boolean isPrimaryKeyColumn(String columnName) {
    return partitionKeyColumnNames.contains(columnName)
        || clusteringKeyColumnNames.contains(columnName);
  }

  public List<ColumnMetadata> getPartitionKey() {
    return partitionKey;
  }

  public boolean isPartitionKeyColumn(String columnName) {
    return partitionKeyColumnNames.contains(columnName);
  }

  public Map<ColumnMetadata, ClusteringOrder> getClusteringKey() {
    return clusteringKey;
  }

  public boolean isClusteringKeyColumn(String columnName) {
    return clusteringKeyColumnNames.contains(columnName);
  }

  public Map<String, ColumnMetadata> getColumns() {
    return columns;
  }

  public Optional<ColumnMetadata> getColumn(String columnName) {
    return Optional.ofNullable(columns.get(columnName));
  }

  public Map<String, IndexMetadata> getIndexes() {
    return indexes;
  }

  public Optional<IndexMetadata> getIndex(String columnName) {
    return Optional.ofNullable(indexes.get(columnName));
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("namespaceName", namespaceName)
        .add("name", name)
        .add("primaryKey", primaryKey)
        .add("partitionKey", partitionKey)
        .add("clusteringKey", clusteringKey)
        .add("columns", columns)
        .add("indexes", indexes)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TableMetadata)) {
      return false;
    }
    TableMetadata that = (TableMetadata) o;
    return Objects.equals(namespaceName, that.namespaceName)
        && Objects.equals(name, that.name)
        && Objects.equals(primaryKey, that.primaryKey)
        && Objects.equals(partitionKey, that.partitionKey)
        && Objects.equals(clusteringKey, that.clusteringKey)
        && Objects.equals(columns, that.columns)
        && Objects.equals(indexes, that.indexes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        namespaceName, name, primaryKey, partitionKey, clusteringKey, columns, indexes);
  }
}
