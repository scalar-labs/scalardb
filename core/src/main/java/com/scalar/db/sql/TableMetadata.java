package com.scalar.db.sql;

import com.google.common.collect.Streams;
import com.scalar.db.api.Scan;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TableMetadata {

  private final String namespaceName;
  private final String name;
  private final com.scalar.db.api.TableMetadata tableMetadata;

  public TableMetadata(
      String namespaceName, String name, com.scalar.db.api.TableMetadata tableMetadata) {
    this.namespaceName = namespaceName;
    this.name = name;
    this.tableMetadata = tableMetadata;
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

  private static DataType convertDataType(com.scalar.db.io.DataType dataType) {
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

  private static ClusteringOrder convertClusteringOrder(Scan.Ordering.Order order) {
    switch (order) {
      case ASC:
        return ClusteringOrder.ASC;
      case DESC:
        return ClusteringOrder.DESC;
      default:
        throw new AssertionError();
    }
  }
}
