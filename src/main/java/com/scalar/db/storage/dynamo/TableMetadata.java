package com.scalar.db.storage.dynamo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.stream.Collectors;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * A metadata class for a table of Scalar DB to know the type of each column
 *
 * @author Yuji Ito
 */
public class TableMetadata {
  private static final String PARTITION_KEY = "partitionKey";
  private static final String CLUSTERING_KEY = "clusteringKey";
  private static final String SORT_KEY = "sortKey";
  private static final String COLUMNS = "columns";
  private SortedSet<String> partitionKeyNames;
  private SortedSet<String> clusteringKeyNames;
  private Optional<String> sortKeyName;
  private SortedMap<String, String> columns;
  private List<String> keyNames;

  public TableMetadata(Map<String, AttributeValue> metadata) {
    convert(metadata);
  }

  public Set<String> getPartitionKeyNames() {
    return partitionKeyNames;
  }

  public Set<String> getClusteringKeyNames() {
    return clusteringKeyNames;
  }

  public Optional<String> getSortKeyName() {
    return sortKeyName;
  }

  public Map<String, String> getColumns() {
    return columns;
  }

  public List<String> getKeyNames() {
    if (keyNames != null) {
      return keyNames;
    }

    keyNames =
        new ImmutableList.Builder<String>()
            .addAll(partitionKeyNames)
            .addAll(clusteringKeyNames)
            .build();

    return keyNames;
  }

  private void convert(Map<String, AttributeValue> metadata) {
    this.partitionKeyNames = ImmutableSortedSet.copyOf(metadata.get(PARTITION_KEY).ss());
    this.clusteringKeyNames = ImmutableSortedSet.copyOf(metadata.get(CLUSTERING_KEY).ss());
    this.sortKeyName = Optional.ofNullable(metadata.get(SORT_KEY).s());

    SortedMap<String, String> cs =
        metadata.get(COLUMNS).m().entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, e -> e.getValue().s(), (u, v) -> v, TreeMap::new));
    this.columns = Collections.unmodifiableSortedMap(cs);
  }
}
