package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
public class ObjectStorageTableMetadata {
  private final LinkedHashSet<String> partitionKeyNames;
  private final LinkedHashSet<String> clusteringKeyNames;
  private final Map<String, String> clusteringOrders;
  private final Set<String> secondaryIndexNames;
  private final Map<String, String> columns;

  // The default constructor is required by Jackson to deserialize JSON object
  public ObjectStorageTableMetadata() {
    this(null, null, null, null, null);
  }

  public ObjectStorageTableMetadata(
      @Nullable LinkedHashSet<String> partitionKeyNames,
      @Nullable LinkedHashSet<String> clusteringKeyNames,
      @Nullable Map<String, String> clusteringOrders,
      @Nullable Set<String> secondaryIndexNames,
      @Nullable Map<String, String> columns) {
    this.partitionKeyNames = partitionKeyNames != null ? partitionKeyNames : new LinkedHashSet<>();
    this.clusteringKeyNames =
        clusteringKeyNames != null ? clusteringKeyNames : new LinkedHashSet<>();
    this.clusteringOrders = clusteringOrders != null ? clusteringOrders : Collections.emptyMap();
    this.secondaryIndexNames =
        secondaryIndexNames != null ? secondaryIndexNames : Collections.emptySet();
    this.columns = columns != null ? columns : Collections.emptyMap();
  }

  public ObjectStorageTableMetadata(TableMetadata tableMetadata) {
    Map<String, String> clusteringOrders =
        tableMetadata.getClusteringKeyNames().stream()
            .collect(Collectors.toMap(c -> c, c -> tableMetadata.getClusteringOrder(c).name()));
    Map<String, String> columnTypeByName = new HashMap<>();
    tableMetadata
        .getColumnNames()
        .forEach(
            columnName ->
                columnTypeByName.put(
                    columnName, tableMetadata.getColumnDataType(columnName).name().toLowerCase()));
    this.partitionKeyNames = tableMetadata.getPartitionKeyNames();
    this.clusteringKeyNames = tableMetadata.getClusteringKeyNames();
    this.clusteringOrders = clusteringOrders;
    this.secondaryIndexNames = tableMetadata.getSecondaryIndexNames();
    this.columns = columnTypeByName;
  }

  public LinkedHashSet<String> getPartitionKeyNames() {
    return partitionKeyNames;
  }

  public LinkedHashSet<String> getClusteringKeyNames() {
    return clusteringKeyNames;
  }

  public Map<String, String> getClusteringOrders() {
    return clusteringOrders;
  }

  public Set<String> getSecondaryIndexNames() {
    return secondaryIndexNames;
  }

  public Map<String, String> getColumns() {
    return columns;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ObjectStorageTableMetadata)) {
      return false;
    }
    ObjectStorageTableMetadata that = (ObjectStorageTableMetadata) o;
    return Objects.equals(partitionKeyNames, that.partitionKeyNames)
        && Objects.equals(clusteringKeyNames, that.clusteringKeyNames)
        && Objects.equals(clusteringOrders, that.clusteringOrders)
        && Objects.equals(secondaryIndexNames, that.secondaryIndexNames)
        && Objects.equals(columns, that.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        partitionKeyNames, clusteringKeyNames, clusteringOrders, secondaryIndexNames, columns);
  }

  public TableMetadata toTableMetadata() {
    TableMetadata.Builder builder = TableMetadata.newBuilder();
    partitionKeyNames.forEach(builder::addPartitionKey);
    clusteringKeyNames.forEach(
        n -> builder.addClusteringKey(n, Scan.Ordering.Order.valueOf(clusteringOrders.get(n))));
    secondaryIndexNames.forEach(builder::addSecondaryIndex);
    columns.forEach((key, value) -> builder.addColumn(key, convertDataType(value)));
    return builder.build();
  }

  private DataType convertDataType(String columnType) {
    switch (columnType) {
      case "int":
        return DataType.INT;
      case "bigint":
        return DataType.BIGINT;
      case "float":
        return DataType.FLOAT;
      case "double":
        return DataType.DOUBLE;
      case "text":
        return DataType.TEXT;
      case "boolean":
        return DataType.BOOLEAN;
      case "blob":
        return DataType.BLOB;
      case "date":
        return DataType.DATE;
      case "time":
        return DataType.TIME;
      case "timestamp":
        return DataType.TIMESTAMP;
      case "timestamptz":
        return DataType.TIMESTAMPTZ;
      default:
        throw new AssertionError("Unknown column type: " + columnType);
    }
  }
}
