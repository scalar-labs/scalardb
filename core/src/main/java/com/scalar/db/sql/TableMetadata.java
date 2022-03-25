package com.scalar.db.sql;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Streams;
import com.scalar.db.api.Scan;
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
  private final com.scalar.db.api.TableMetadata tableMetadata;

  TableMetadata(String namespaceName, String name, com.scalar.db.api.TableMetadata tableMetadata) {
    this.namespaceName = Objects.requireNonNull(namespaceName);
    this.name = Objects.requireNonNull(name);
    this.tableMetadata = Objects.requireNonNull(tableMetadata);
  }

  public String getNamespaceName() {
    return namespaceName;
  }

  public String getName() {
    return name;
  }

  public List<ColumnMetadata> getPrimaryKey() {
    return Streams.concat(
            tableMetadata.getPartitionKeyNames().stream(),
            tableMetadata.getClusteringKeyNames().stream())
        .map(
            c ->
                new ColumnMetadata(
                    namespaceName, name, c, convertDataType(tableMetadata.getColumnDataType(c))))
        .collect(Collectors.toList());
  }

  public List<ColumnMetadata> getPartitionKey() {
    return tableMetadata.getPartitionKeyNames().stream()
        .map(
            c ->
                new ColumnMetadata(
                    namespaceName, name, c, convertDataType(tableMetadata.getColumnDataType(c))))
        .collect(Collectors.toList());
  }

  public Map<ColumnMetadata, ClusteringOrder> getClusteringKey() {
    return tableMetadata.getClusteringKeyNames().stream()
        .map(
            c ->
                new ColumnMetadata(
                    namespaceName, name, c, convertDataType(tableMetadata.getColumnDataType(c))))
        .collect(
            Collectors.toMap(
                Function.identity(),
                c -> convertClusteringOrder(tableMetadata.getClusteringOrder(c.getName()))));
  }

  public Map<String, ColumnMetadata> getColumns() {
    return tableMetadata.getColumnNames().stream()
        .map(
            c ->
                new ColumnMetadata(
                    namespaceName, name, c, convertDataType(tableMetadata.getColumnDataType(c))))
        .collect(Collectors.toMap(ColumnMetadata::getName, Function.identity()));
  }

  public Optional<ColumnMetadata> getColumn(String columnName) {
    if (!tableMetadata.getColumnNames().contains(columnName)) {
      return Optional.empty();
    }
    return Optional.of(
        new ColumnMetadata(
            namespaceName,
            name,
            columnName,
            convertDataType(tableMetadata.getColumnDataType(columnName))));
  }

  public Map<String, IndexMetadata> getIndexes() {
    return tableMetadata.getSecondaryIndexNames().stream()
        .collect(
            Collectors.toMap(Function.identity(), c -> new IndexMetadata(namespaceName, name, c)));
  }

  public Optional<IndexMetadata> getIndex(String columnName) {
    if (!tableMetadata.getSecondaryIndexNames().contains(columnName)) {
      return Optional.empty();
    }
    return Optional.of(new IndexMetadata(namespaceName, name, columnName));
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

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("namespaceName", namespaceName)
        .add("name", name)
        .add("partitionKey", getPartitionKey())
        .add("clusteringKey", getClusteringKey())
        .add("columns", getColumns())
        .add("indexes", getIndexes())
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
        && Objects.equals(tableMetadata, that.tableMetadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespaceName, name, tableMetadata);
  }
}
