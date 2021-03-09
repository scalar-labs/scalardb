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

public class OperationChecker {

  private final TableMetadata metadata;

  public OperationChecker(TableMetadata metadata) {
    this.metadata = metadata;
  }

  public void check(Get get) {
    checkProjections(get);

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

    checkPrimaryKey(get);
  }

  public void check(Scan scan) {
    checkProjections(scan);

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

    checkPartitionKey(scan);

    checkClusteringKeys(scan);

    if (scan.getLimit() < 0) {
      throw new IllegalArgumentException("The limit cannot be negative Operation: " + scan);
    }

    checkOrderings(scan);
  }

  private void checkProjections(Selection selection) {
    for (String projection : selection.getProjections()) {
      if (!metadata.getColumnNames().contains(projection)) {
        throw new IllegalArgumentException(
            "The specified projection is not found. Operation: " + selection);
      }
    }
  }

  private void checkClusteringKeys(Scan scan) {
    scan.getStartClusteringKey().ifPresent(startClusteringKey -> checkStartClusteringKey(scan));

    scan.getEndClusteringKey().ifPresent(endClusteringKey -> checkEndClusteringKey(scan));

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

  private void checkStartClusteringKey(Scan scan) {
    scan.getStartClusteringKey()
        .ifPresent(
            ckey -> {
              if (!checkKey(ckey, metadata.getClusteringKeyNames(), true)) {
                throw new IllegalArgumentException(
                    "The start clustering key is not properly specified. Operation: " + scan);
              }
            });
  }

  private void checkEndClusteringKey(Scan scan) {
    scan.getEndClusteringKey()
        .ifPresent(
            ckey -> {
              if (!checkKey(ckey, metadata.getClusteringKeyNames(), true)) {
                throw new IllegalArgumentException(
                    "The end clustering key is not properly specified. Operation: " + scan);
              }
            });
  }

  private void checkOrderings(Scan scan) {
    List<Scan.Ordering> orderings = scan.getOrderings();
    if (orderings.isEmpty()) {
      return;
    }

    Supplier<String> message = () -> "Orderings are not properly specified. Operation: " + scan;

    if (orderings.size() > metadata.getClusteringKeyNames().size()) {
      throw new IllegalArgumentException(message.get());
    }

    Boolean reverse = null;
    Iterator<String> iterator = metadata.getClusteringKeyNames().iterator();
    for (Scan.Ordering ordering : orderings) {
      String clusteringKeyName = iterator.next();
      if (!ordering.getName().equals(clusteringKeyName)) {
        throw new IllegalArgumentException(message.get());
      }

      boolean rightOrder = ordering.getOrder() != metadata.getClusteringOrder(ordering.getName());
      if (reverse == null) {
        reverse = rightOrder;
      } else {
        if (reverse != rightOrder) {
          throw new IllegalArgumentException(message.get());
        }
      }
    }
  }

  public void check(Mutation mutation) {
    if (mutation instanceof Put) {
      check((Put) mutation);
    } else {
      check((Delete) mutation);
    }
  }

  public void check(Put put) {
    checkPrimaryKey(put);
    checkValues(put);
    checkCondition(put);
  }

  public void check(Delete delete) {
    checkPrimaryKey(delete);
    checkCondition(delete);
  }

  private void checkValues(Put put) {
    for (Map.Entry<String, Value> entry : put.getValues().entrySet()) {
      if (!new ColumnChecker(metadata).check(entry.getValue())) {
        throw new IllegalArgumentException(
            "The values are not properly specified. Operation: " + put);
      }
    }
  }

  private void checkCondition(Mutation mutation) {
    boolean isPut = mutation instanceof Put;
    mutation
        .getCondition()
        .ifPresent(
            c -> {
              if (!new ConditionChecker(metadata).check(mutation.getCondition().get(), isPut)) {
                throw new IllegalArgumentException(
                    "The condition is not properly specified. Operation: " + mutation);
              }
            });
  }

  public void check(List<? extends Mutation> mutations) {
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

  private void checkPrimaryKey(Operation operation) {
    checkPartitionKey(operation);
    checkClusteringKey(operation);
  }

  private void checkPartitionKey(Operation operation) {
    if (!checkKey(operation.getPartitionKey(), metadata.getPartitionKeyNames(), false)) {
      throw new IllegalArgumentException(
          "The partition key is not properly specified. Operation: " + operation);
    }
  }

  private void checkClusteringKey(Operation operation) {
    Supplier<String> message =
        () -> "The clustering key is not properly specified. Operation: " + operation;

    if (!metadata.getClusteringKeyNames().isEmpty() && !operation.getClusteringKey().isPresent()) {
      throw new IllegalArgumentException(message.get());
    }

    operation
        .getClusteringKey()
        .ifPresent(
            ckey -> {
              if (!checkKey(ckey, metadata.getClusteringKeyNames(), false)) {
                throw new IllegalArgumentException(message.get());
              }
            });
  }

  private boolean checkKey(Key key, LinkedHashSet<String> keyNames, boolean allowPartial) {
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

      if (!new ColumnChecker(metadata).check(value)) {
        return false;
      }
    }

    return true;
  }
}
