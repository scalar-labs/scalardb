package com.scalar.db.storage.dynamo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.Scan;
import com.scalar.db.exception.storage.UnsupportedTypeException;
import com.scalar.db.storage.common.metadata.DataType;
import com.scalar.db.storage.common.metadata.TableMetadata;
import com.scalar.db.storage.common.util.ImmutableLinkedHashSet;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A metadata class for a table of Scalar DB to know the type of each column
 *
 * @author Yuji Ito
 */
public class DynamoTableMetadata implements TableMetadata {
  private static final String PARTITION_KEY = "partitionKey";
  private static final String CLUSTERING_KEY = "clusteringKey";
  private static final String SECONDARY_INDEX = "secondaryIndex";
  private static final String COLUMNS = "columns";
  private LinkedHashSet<String> partitionKeyNames;
  private LinkedHashSet<String> clusteringKeyNames;
  private Set<String> secondaryIndexNames;
  private Map<String, DataType> columnDataTypes;
  private List<String> keyNames;

  public DynamoTableMetadata(Map<String, AttributeValue> metadata) {
    convert(metadata);
  }

  @Override
  public LinkedHashSet<String> getPartitionKeyNames() {
    return partitionKeyNames;
  }

  @Override
  public LinkedHashSet<String> getClusteringKeyNames() {
    return clusteringKeyNames;
  }

  @Override
  public Set<String> getSecondaryIndexNames() {
    return secondaryIndexNames;
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
    // Always returns ASC for now if the clustering key name exists
    return clusteringKeyNames.contains(clusteringKeyName) ? Scan.Ordering.Order.ASC : null;
  }

  public List<String> getKeyNames() {
    return keyNames;
  }

  private void convert(Map<String, AttributeValue> metadata) {
    this.partitionKeyNames =
        new ImmutableLinkedHashSet<>(
            metadata.get(PARTITION_KEY).l().stream()
                .map(AttributeValue::s)
                .collect(Collectors.toList()));
    if (metadata.containsKey(CLUSTERING_KEY)) {
      this.clusteringKeyNames =
          new ImmutableLinkedHashSet<>(
              metadata.get(CLUSTERING_KEY).l().stream()
                  .map(AttributeValue::s)
                  .collect(Collectors.toList()));
    } else {
      this.clusteringKeyNames = new ImmutableLinkedHashSet<>();
    }
    if (metadata.containsKey(SECONDARY_INDEX)) {
      this.secondaryIndexNames = ImmutableSet.copyOf(metadata.get(SECONDARY_INDEX).ss());
    } else {
      this.secondaryIndexNames = ImmutableSet.of();
    }

    this.keyNames =
        new ImmutableList.Builder<String>()
            .addAll(partitionKeyNames)
            .addAll(clusteringKeyNames)
            .build();

    columnDataTypes =
        ImmutableMap.copyOf(
            metadata.get(COLUMNS).m().entrySet().stream()
                .collect(
                    Collectors.toMap(Map.Entry::getKey, e -> convertDataType(e.getValue().s()))));
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
      case "text": // for backwards compatibility
      case "varchar":
        return DataType.TEXT;
      case "boolean":
        return DataType.BOOLEAN;
      case "blob":
        return DataType.BLOB;
      default:
        throw new UnsupportedTypeException(columnType);
    }
  }
}
