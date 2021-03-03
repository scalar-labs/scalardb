package com.scalar.db.storage.common.checker;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Selection;
import com.scalar.db.exception.storage.MultiPartitionException;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.storage.common.metadata.TableMetadata;
import com.scalar.db.storage.common.util.Utility;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public final class OperationChecker {
  private OperationChecker() {}

  public static void check(Get get, TableMetadata metadata) {
    checkProjections(get, metadata);

    if (Utility.isSecondaryIndexSpecified(get, metadata)) {
      if (!new ColumnChecker(metadata).check(get.getPartitionKey().get().get(0))) {
        throw new IllegalArgumentException(
            "The partition key is not properly specified. Operation: " + get);
      }

      if (get.getClusteringKey().isPresent()) {
        throw new IllegalArgumentException(
            "Clustering keys cannot be specified when using an index. Operation: " + get);
      }
      return;
    }

    checkPrimaryKey(get, metadata);
  }

  public static void check(Scan scan, TableMetadata metadata) {
    checkProjections(scan, metadata);

    if (Utility.isSecondaryIndexSpecified(scan, metadata)) {
      if (!new ColumnChecker(metadata).check(scan.getPartitionKey().get().get(0))) {
        throw new IllegalArgumentException(
            "The partition key is not properly specified. Operation: " + scan);
      }

      if (scan.getStartClusteringKey().isPresent() || scan.getEndClusteringKey().isPresent()) {
        throw new IllegalArgumentException(
            "Clustering keys cannot be specified when using an index. Operation: " + scan);
      }
      if (!scan.getOrderings().isEmpty()) {
        throw new IllegalArgumentException(
            "Orderings cannot be specified when using an index. Operation: " + scan);
      }
      return;
    }

    checkPartitionKey(scan, metadata);

    checkClusteringKeys(scan, metadata);

    if (scan.getLimit() < 0) {
      throw new IllegalArgumentException("The limit cannot be negative Operation: " + scan);
    }

    checkOrderings(scan, metadata);
  }

  private static void checkProjections(Selection selection, TableMetadata tableMetadata) {
    for (String projection : selection.getProjections()) {
      if (!tableMetadata.getColumnNames().contains(projection)) {
        throw new IllegalArgumentException(
            "The specified projection is not found. Operation: " + selection);
      }
    }
  }

  private static void checkClusteringKeys(Scan scan, TableMetadata metadata) {
    scan.getStartClusteringKey()
        .ifPresent(startClusteringKey -> checkStartClusteringKey(scan, metadata));

    scan.getEndClusteringKey().ifPresent(endClusteringKey -> checkEndClusteringKey(scan, metadata));

    if (scan.getStartClusteringKey().isPresent() && scan.getEndClusteringKey().isPresent()) {
      Key startClusteringKey = scan.getStartClusteringKey().get();
      Key endClusteringKey = scan.getEndClusteringKey().get();
      Supplier<String> message =
          () -> "The clustering key range is not properly specified. Operation: " + scan;

      if (startClusteringKey.size() != endClusteringKey.size()) {
        throw new IllegalArgumentException(message.get());
      }

      for (int i = 0; i < startClusteringKey.size() - 1; i++) {
        Value startValue = startClusteringKey.get().get(i);
        Value endValue = endClusteringKey.get().get(i);
        if (!startValue.equals(endValue)) {
          throw new IllegalArgumentException(message.get());
        }
      }
    }
  }

  private static void checkStartClusteringKey(Scan scan, TableMetadata metadata) {
    scan.getStartClusteringKey()
        .ifPresent(
            ckey -> {
              if (!checkKey(ckey, metadata.getClusteringKeyNames(), true, metadata)) {
                throw new IllegalArgumentException(
                    "The start clustering key is not properly specified. Operation: " + scan);
              }
            });
  }

  private static void checkEndClusteringKey(Scan scan, TableMetadata metadata) {
    scan.getEndClusteringKey()
        .ifPresent(
            ckey -> {
              if (!checkKey(ckey, metadata.getClusteringKeyNames(), true, metadata)) {
                throw new IllegalArgumentException(
                    "The end clustering key is not properly specified. Operation: " + scan);
              }
            });
  }

  private static void checkOrderings(Scan scan, TableMetadata tableMetadata) {
    List<Scan.Ordering> orderings = scan.getOrderings();
    if (orderings.isEmpty()) {
      return;
    }

    Supplier<String> message = () -> "Orderings are not properly specified. Operation: " + scan;

    if (orderings.size() > tableMetadata.getClusteringKeyNames().size()) {
      throw new IllegalArgumentException(message.get());
    }

    Boolean reverse = null;
    Iterator<String> iterator = tableMetadata.getClusteringKeyNames().iterator();
    for (Scan.Ordering ordering : orderings) {
      String clusteringKeyName = iterator.next();
      if (!ordering.getName().equals(clusteringKeyName)) {
        throw new IllegalArgumentException(message.get());
      }

      boolean rightOrder =
          ordering.getOrder() != tableMetadata.getClusteringOrder(ordering.getName());
      if (reverse == null) {
        reverse = rightOrder;
      } else {
        if (reverse != rightOrder) {
          throw new IllegalArgumentException(message.get());
        }
      }
    }
  }

  public static void check(Mutation mutation, TableMetadata metadata) {
    if (mutation instanceof Put) {
      check((Put) mutation, metadata);
    } else {
      check((Delete) mutation, metadata);
    }
  }

  public static void check(Put put, TableMetadata metadata) {
    checkPrimaryKey(put, metadata);
    checkValues(put, metadata);
    checkCondition(put, metadata);
  }

  public static void check(Delete delete, TableMetadata metadata) {
    checkPrimaryKey(delete, metadata);
    checkCondition(delete, metadata);
  }

  private static void checkValues(Put put, TableMetadata metadata) {
    for (Map.Entry<String, Value> entry : put.getValues().entrySet()) {
      if (!new ColumnChecker(metadata).check(entry.getValue())) {
        throw new IllegalArgumentException(
            "The values are not properly specified. Operation: " + put);
      }
    }
  }

  private static void checkCondition(Mutation mutation, TableMetadata tableMetadata) {
    boolean isPut = mutation instanceof Put;
    mutation
        .getCondition()
        .ifPresent(
            c -> {
              if (!new ConditionChecker(tableMetadata)
                  .check(mutation.getCondition().get(), isPut)) {
                throw new IllegalArgumentException(
                    "The condition is not properly specified. Operation: " + mutation);
              }
            });
  }

  public static void check(List<? extends Mutation> mutations) {
    if (mutations.isEmpty()) {
      throw new IllegalArgumentException("The mutations are empty");
    }

    Mutation first = mutations.get(0);
    for (Mutation mutation : mutations) {
      if (!mutation.forTable().equals(first.forTable())
          || !mutation.getPartitionKey().equals(first.getPartitionKey())) {
        throw new MultiPartitionException(
            "decided not to execute this batch since multi-partition batch is not recommended");
      }
    }
  }

  private static void checkPrimaryKey(Operation operation, TableMetadata metadata) {
    checkPartitionKey(operation, metadata);
    checkClusteringKey(operation, metadata);
  }

  private static void checkPartitionKey(Operation operation, TableMetadata metadata) {
    if (!checkKey(operation.getPartitionKey(), metadata.getPartitionKeyNames(), false, metadata)) {
      throw new IllegalArgumentException(
          "The partition key is not properly specified. Operation: " + operation);
    }
  }

  private static void checkClusteringKey(Operation operation, TableMetadata metadata) {
    Supplier<String> message =
        () -> "The clustering key is not properly specified. Operation: " + operation;

    if (!metadata.getClusteringKeyNames().isEmpty() && !operation.getClusteringKey().isPresent()) {
      throw new IllegalArgumentException(message.get());
    }

    operation
        .getClusteringKey()
        .ifPresent(
            ckey -> {
              if (!checkKey(ckey, metadata.getClusteringKeyNames(), false, metadata)) {
                throw new IllegalArgumentException(message.get());
              }
            });
  }

  private static boolean checkKey(
      Key key, LinkedHashSet<String> keyNames, boolean allowPartial, TableMetadata tableMetadata) {
    List<Value> values = new ArrayList<>(key.get());

    if (!allowPartial) {
      if (values.size() != keyNames.size()) {
        return false;
      }
    } else {
      if (values.size() > keyNames.size()) {
        return false;
      }
    }

    Iterator<String> iterator = keyNames.iterator();
    for (Value value : values) {
      String keyName = iterator.next();

      if (!keyName.equals(value.getName())) {
        return false;
      }

      if (!new ColumnChecker(tableMetadata).check(value)) {
        return false;
      }
    }

    return true;
  }
}
