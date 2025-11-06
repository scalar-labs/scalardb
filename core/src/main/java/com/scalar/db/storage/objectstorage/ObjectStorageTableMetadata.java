package com.scalar.db.storage.objectstorage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
@Immutable
public class ObjectStorageTableMetadata {
  private final LinkedHashSet<String> partitionKeyNames;
  private final LinkedHashSet<String> clusteringKeyNames;
  private final Map<String, String> clusteringOrders;
  private final Set<String> secondaryIndexNames;
  private final Map<String, String> columns;

  @JsonCreator
  public ObjectStorageTableMetadata(
      @JsonProperty("partitionKeyNames") @Nullable LinkedHashSet<String> partitionKeyNames,
      @JsonProperty("clusteringKeyNames") @Nullable LinkedHashSet<String> clusteringKeyNames,
      @JsonProperty("clusteringOrders") @Nullable Map<String, String> clusteringOrders,
      @JsonProperty("secondaryIndexNames") @Nullable Set<String> secondaryIndexNames,
      @JsonProperty("columns") @Nullable Map<String, String> columns) {
    this.partitionKeyNames =
        partitionKeyNames != null ? new LinkedHashSet<>(partitionKeyNames) : new LinkedHashSet<>();
    this.clusteringKeyNames =
        clusteringKeyNames != null
            ? new LinkedHashSet<>(clusteringKeyNames)
            : new LinkedHashSet<>();
    this.clusteringOrders =
        clusteringOrders != null ? new HashMap<>(clusteringOrders) : Collections.emptyMap();
    this.secondaryIndexNames =
        secondaryIndexNames != null ? new HashSet<>(secondaryIndexNames) : Collections.emptySet();
    this.columns = columns != null ? new HashMap<>(columns) : Collections.emptyMap();
  }

  public ObjectStorageTableMetadata(TableMetadata tableMetadata) {
    Map<String, String> clusteringOrders =
        tableMetadata.getClusteringKeyNames().stream()
            .collect(
                Collectors.toMap(
                    Function.identity(), c -> tableMetadata.getClusteringOrder(c).name()));
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

  private ObjectStorageTableMetadata(Builder builder) {
    this(
        builder.partitionKeyNames,
        builder.clusteringKeyNames,
        builder.clusteringOrders,
        builder.secondaryIndexNames,
        builder.columns);
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

  public static ObjectStorageTableMetadata.Builder newBuilder() {
    return new ObjectStorageTableMetadata.Builder();
  }

  public static final class Builder {
    private LinkedHashSet<String> partitionKeyNames;
    private LinkedHashSet<String> clusteringKeyNames;
    private Map<String, String> clusteringOrders;
    private Set<String> secondaryIndexNames;
    private Map<String, String> columns;

    private Builder() {}

    public ObjectStorageTableMetadata.Builder partitionKeyNames(LinkedHashSet<String> val) {
      partitionKeyNames = val;
      return this;
    }

    public ObjectStorageTableMetadata.Builder clusteringKeyNames(LinkedHashSet<String> val) {
      clusteringKeyNames = val;
      return this;
    }

    public ObjectStorageTableMetadata.Builder clusteringOrders(Map<String, String> val) {
      clusteringOrders = val;
      return this;
    }

    public ObjectStorageTableMetadata.Builder secondaryIndexNames(Set<String> val) {
      secondaryIndexNames = val;
      return this;
    }

    public ObjectStorageTableMetadata.Builder columns(Map<String, String> val) {
      columns = val;
      return this;
    }

    public ObjectStorageTableMetadata build() {
      return new ObjectStorageTableMetadata(this);
    }
  }
}
