package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.Result;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.storage.common.ResultImpl;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

public class TableSnapshot {

  private final TableMetadata metadata;
  private final Map<Key, NavigableMap<Optional<Key>, Map<String, Value<?>>>> data;
  private final Comparator<Optional<Key>> clusteringKeyComparator;

  public TableSnapshot(TableMetadata metadata) {
    this.metadata = metadata;
    data = new HashMap<>();
    clusteringKeyComparator = getClusteringKeyComparator(metadata);
  }

  private Comparator<Optional<Key>> getClusteringKeyComparator(TableMetadata metadata) {
    return (left, right) -> {
      if (!left.isPresent() && !right.isPresent()) {
        return 0;
      } else if (!left.isPresent()) {
        return 1;
      } else if (!right.isPresent()) {
        return -1;
      }

      Iterator<Value<?>> leftValuesIterator = left.get().get().iterator();
      Iterator<Value<?>> rightValuesIterator = right.get().get().iterator();
      while (leftValuesIterator.hasNext()) {
        if (!rightValuesIterator.hasNext()) {
          return 1; // because it's longer
        }
        Value<?> leftValue = leftValuesIterator.next();
        Value<?> rightValue = rightValuesIterator.next();
        Comparator<Value<?>> comparator =
            metadata.getClusteringOrder(leftValue.getName()) == Order.ASC
                ? com.google.common.collect.Ordering.natural()
                : com.google.common.collect.Ordering.natural().reverse();
        int result = comparator.compare(leftValue, rightValue);
        if (result != 0) {
          return result;
        }
      }
      if (rightValuesIterator.hasNext()) {
        return -1; // because it's longer
      }
      return 0;
    };
  }

  public Optional<Result> get(Key partitionKey, Optional<Key> clusteringKey) {
    Map<String, Value<?>> values = getDataInPartition(partitionKey).get(clusteringKey);
    if (values == null) {
      return Optional.empty();
    }
    return Optional.of(new ResultImpl(values, metadata));
  }

  public List<Result> scan(
      Key partitionKey,
      Optional<Key> startClusteringKey,
      boolean startInclusive,
      Optional<Key> endClusteringKey,
      boolean endInclusive,
      List<Ordering> orderings,
      int limit) {
    // If it's a scan for DESC clustering order, flip the startClusteringKey and the
    // endClusteringKey
    if (isScanForDescClusteringOrder(startClusteringKey, endClusteringKey)) {
      Optional<Key> tmpClusteringKey = startClusteringKey;
      startClusteringKey = endClusteringKey;
      endClusteringKey = tmpClusteringKey;

      boolean tmpInclusive = startInclusive;
      startInclusive = endInclusive;
      endInclusive = tmpInclusive;
    }

    NavigableMap<Optional<Key>, Map<String, Value<?>>> data = getDataInPartition(partitionKey);

    if (startClusteringKey.isPresent()) {
      // We can use tailMap for startClusteringKey
      data = data.tailMap(startClusteringKey, startInclusive);

      // If startClusteringKey consists of multiple values and endClusteringKey is not present, use
      // startClusteringKey without the last value for endClusteringKey
      if (startClusteringKey.get().get().size() > 1 && !endClusteringKey.isPresent()) {
        endClusteringKey = Optional.of(getKeyWithoutLastValue(startClusteringKey.get()));
      }
    } else if (endClusteringKey.isPresent()) {
      // If endClusteringKey consists of multiple values and startClusteringKey is not present, use
      // endClusteringKey without the last value for startClusteringKey
      if (endClusteringKey.get().get().size() > 1) {
        startClusteringKey = Optional.of(getKeyWithoutLastValue(endClusteringKey.get()));
      }
    }

    List<Result> ret = new ArrayList<>();
    for (Entry<Optional<Key>, Map<String, Value<?>>> entry : data.entrySet()) {
      Optional<Key> clusteringKey = entry.getKey();

      // Skip the data if the clusteringKey is less than (or equal to) startClusteringKey
      if (startClusteringKey.isPresent()) {
        int compare = clusteringKeyComparator.compare(clusteringKey, startClusteringKey);
        if (compare < 0 || (compare == 0 && !startInclusive)) {
          continue;
        }
      }

      // Stop scanning if the clusteringKey is more than (or equal to) endClusteringKey
      if (endClusteringKey.isPresent()) {
        assert clusteringKey.isPresent();
        boolean fullClusteringKeySpecified =
            metadata.getClusteringKeyNames().size() == endClusteringKey.get().size();

        int compare = clusteringKeyComparator.compare(clusteringKey, endClusteringKey);
        if (fullClusteringKeySpecified) {
          if (compare > 0 || (compare == 0 && !endInclusive)) {
            break;
          }
        } else {
          if (compare > 0) {
            if (endInclusive) {
              if (!clusteringKey
                  .get()
                  .get()
                  .subList(0, endClusteringKey.get().size())
                  .equals(endClusteringKey.get().get())) {
                break;
              }
            } else {
              break;
            }
          }
        }
      }

      // Otherwise, add the data to the result
      ret.add(new ResultImpl(entry.getValue(), metadata));
    }

    if (!orderings.isEmpty()) {
      Ordering ordering = orderings.get(0);
      if (ordering.getOrder() != metadata.getClusteringOrder(ordering.getName())) {
        // reverse scan
        Collections.reverse(ret);
      }
    }

    return (limit > 0 && ret.size() > limit) ? ret.subList(0, limit) : ret;
  }

  private boolean isScanForDescClusteringOrder(
      Optional<Key> startClusteringKey, Optional<Key> endClusteringKey) {
    if (startClusteringKey.isPresent()) {
      Key key = startClusteringKey.get();
      String lastValueName = key.get().get(key.size() - 1).getName();
      return metadata.getClusteringOrder(lastValueName) == Order.DESC;
    }
    if (endClusteringKey.isPresent()) {
      Key key = endClusteringKey.get();
      String lastValueName = key.get().get(key.size() - 1).getName();
      return metadata.getClusteringOrder(lastValueName) == Order.DESC;
    }
    return false;
  }

  private Key getKeyWithoutLastValue(Key originalKey) {
    Key.Builder keyBuilder = Key.newBuilder();
    for (int i = 0; i < originalKey.get().size() - 1; i++) {
      keyBuilder.add(originalKey.get().get(i));
    }
    return keyBuilder.build();
  }

  public void put(Key partitionKey, Optional<Key> clusteringKey, Map<String, Value<?>> values) {
    if (getDataInPartition(partitionKey).containsKey(clusteringKey)) {
      getDataInPartition(partitionKey).get(clusteringKey).putAll(values);
    } else {
      // merge the values for the previous and the new
      Map<String, Value<?>> valueMap = new HashMap<>(values);
      partitionKey.get().forEach(v -> valueMap.put(v.getName(), v));
      clusteringKey.ifPresent(c -> c.forEach(v -> valueMap.put(v.getName(), v)));
      getDataInPartition(partitionKey).put(clusteringKey, valueMap);
    }
  }

  public void delete(Key partitionKey, Optional<Key> clusteringKey) {
    getDataInPartition(partitionKey).remove(clusteringKey);
  }

  private NavigableMap<Optional<Key>, Map<String, Value<?>>> getDataInPartition(Key partitionKey) {
    if (!data.containsKey(partitionKey)) {
      data.put(partitionKey, new TreeMap<>(clusteringKeyComparator));
    }
    return data.get(partitionKey);
  }
}
