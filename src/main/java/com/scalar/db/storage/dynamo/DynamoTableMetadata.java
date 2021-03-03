package com.scalar.db.storage.dynamo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.scalar.db.storage.ImmutableLinkedHashSet;
import com.scalar.db.storage.TableMetadata;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
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
  private SortedSet<String> secondayIndexNames;
  private SortedMap<String, String> columns;
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
    return secondayIndexNames;
  }

  public Map<String, String> getColumns() {
    return columns;
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
      this.secondayIndexNames = ImmutableSortedSet.copyOf(metadata.get(SECONDARY_INDEX).ss());
    } else {
      this.secondayIndexNames = ImmutableSortedSet.of();
    }

    this.keyNames =
        new ImmutableList.Builder<String>()
            .addAll(partitionKeyNames)
            .addAll(clusteringKeyNames)
            .build();

    SortedMap<String, String> cs =
        metadata.get(COLUMNS).m().entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, e -> e.getValue().s(), (u, v) -> v, TreeMap::new));
    this.columns = Collections.unmodifiableSortedMap(cs);
  }
}
