package com.scalar.db.common.checker;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanAll;
import com.scalar.db.api.Selection;
import com.scalar.db.api.Selection.Conjunction;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.util.ScalarDbUtils;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class OperationChecker {

  private final DatabaseConfig config;
  private final TableMetadataManager tableMetadataManager;

  public OperationChecker(DatabaseConfig config, TableMetadataManager tableMetadataManager) {
    this.config = config;
    this.tableMetadataManager = tableMetadataManager;
  }

  public void check(Get get) throws ExecutionException {
    TableMetadata metadata = getTableMetadata(get);

    checkProjections(get, metadata);

    if (ScalarDbUtils.isSecondaryIndexSpecified(get, metadata)) {
      if (get.getPartitionKey().size() != 1) {
        throw new IllegalArgumentException(
            "Only a single column index is supported. Operation: " + get);
      }

      String name = get.getPartitionKey().getColumns().get(0).getName();
      if (!metadata.getSecondaryIndexNames().contains(name)) {
        throw new IllegalArgumentException(
            "The column of the specified index key is not indexed. Operation: " + get);
      }

      if (!new ColumnChecker(metadata, true, false, false, false)
          .check(get.getPartitionKey().getColumns().get(0))) {
        throw new IllegalArgumentException(
            "The index key is not properly specified. Operation: " + get);
      }

      // The following check is not needed when we use GetWithIndex. But we need to keep it for
      // backward compatibility. We will remove it in release 5.0.0.
      if (get.getClusteringKey().isPresent()) {
        throw new IllegalArgumentException(
            "Clustering keys cannot be specified when using an index. Operation: " + get);
      }
      return;
    }

    checkPrimaryKey(get, metadata);
  }

  public void check(Scan scan) throws ExecutionException {
    if (scan instanceof ScanAll) {
      check((ScanAll) scan);
      return;
    }

    TableMetadata metadata = getTableMetadata(scan);

    checkProjections(scan, metadata);

    if (ScalarDbUtils.isSecondaryIndexSpecified(scan, metadata)) {
      if (scan.getPartitionKey().size() != 1) {
        throw new IllegalArgumentException(
            "Only a single column index is supported. Operation: " + scan);
      }

      String name = scan.getPartitionKey().getColumns().get(0).getName();
      if (!metadata.getSecondaryIndexNames().contains(name)) {
        throw new IllegalArgumentException(
            "The column of the specified index key is not indexed. Operation: " + scan);
      }

      if (!new ColumnChecker(metadata, true, false, false, false)
          .check(scan.getPartitionKey().getColumns().get(0))) {
        throw new IllegalArgumentException(
            "The index key is not properly specified. Operation: " + scan);
      }

      // The following checks are not needed when we use ScanWithIndex. But we need to keep them for
      // backward compatibility. We will remove them in release 5.0.0.
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
      throw new IllegalArgumentException("The limit cannot be negative. Operation: " + scan);
    }

    checkOrderings(scan, metadata);
  }

  private void check(ScanAll scanAll) throws ExecutionException {
    if (!config.isCrossPartitionScanEnabled()) {
      throw new IllegalArgumentException(
          "Cross-partition scan is not enabled. Operation: " + scanAll);
    }

    TableMetadata metadata = getTableMetadata(scanAll);

    checkProjections(scanAll, metadata);

    if (scanAll.getLimit() < 0) {
      throw new IllegalArgumentException("The limit cannot be negative. Operation: " + scanAll);
    }

    if (!config.isCrossPartitionScanOrderingEnabled() && !scanAll.getOrderings().isEmpty()) {
      throw new IllegalArgumentException(
          "Cross-partition scan ordering is not enabled. Operation: " + scanAll);
    }
    checkOrderings(scanAll, metadata);

    if (!config.isCrossPartitionScanFilteringEnabled() && !scanAll.getConjunctions().isEmpty()) {
      throw new IllegalArgumentException(
          "Cross-partition scan filtering is not enabled. Operation: " + scanAll);
    }
    checkConjunctions(scanAll, metadata);
  }

  private void checkProjections(Selection selection, TableMetadata metadata) {
    for (String projection : selection.getProjections()) {
      if (!metadata.getColumnNames().contains(projection)) {
        throw new IllegalArgumentException(
            "The specified projection is not found. Invalid projection: "
                + projection
                + ", Operation: "
                + selection);
      }
    }
  }

  private void checkClusteringKeys(Scan scan, TableMetadata metadata) {
    scan.getStartClusteringKey()
        .ifPresent(startClusteringKey -> checkStartClusteringKey(scan, metadata));

    scan.getEndClusteringKey().ifPresent(endClusteringKey -> checkEndClusteringKey(scan, metadata));

    if (scan.getStartClusteringKey().isPresent() && scan.getEndClusteringKey().isPresent()) {
      Key startClusteringKey = scan.getStartClusteringKey().get();
      Key endClusteringKey = scan.getEndClusteringKey().get();
      Supplier<String> message =
          () -> "The clustering key boundary is not properly specified. Operation: " + scan;

      if (startClusteringKey.size() != endClusteringKey.size()) {
        throw new IllegalArgumentException(message.get());
      }

      for (int i = 0; i < startClusteringKey.size() - 1; i++) {
        Value<?> startValue = startClusteringKey.get().get(i);
        Value<?> endValue = endClusteringKey.get().get(i);
        if (!startValue.equals(endValue)) {
          throw new IllegalArgumentException(message.get());
        }
      }
    }
  }

  private void checkStartClusteringKey(Scan scan, TableMetadata metadata) {
    scan.getStartClusteringKey()
        .ifPresent(
            ckey -> {
              if (!checkKey(ckey, metadata.getClusteringKeyNames(), true, metadata)) {
                throw new IllegalArgumentException(
                    "The start clustering key is not properly specified. Operation: " + scan);
              }
            });
  }

  private void checkEndClusteringKey(Scan scan, TableMetadata metadata) {
    scan.getEndClusteringKey()
        .ifPresent(
            ckey -> {
              if (!checkKey(ckey, metadata.getClusteringKeyNames(), true, metadata)) {
                throw new IllegalArgumentException(
                    "The end clustering key is not properly specified. Operation: " + scan);
              }
            });
  }

  private void checkOrderings(Scan scan, TableMetadata metadata) {
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
      if (!ordering.getColumnName().equals(clusteringKeyName)) {
        throw new IllegalArgumentException(message.get());
      }

      boolean rightOrder =
          ordering.getOrder() != metadata.getClusteringOrder(ordering.getColumnName());
      if (reverse == null) {
        reverse = rightOrder;
      } else {
        if (reverse != rightOrder) {
          throw new IllegalArgumentException(message.get());
        }
      }
    }
  }

  private void checkOrderings(ScanAll scan, TableMetadata metadata) {
    for (Scan.Ordering ordering : scan.getOrderings()) {
      if (!metadata.getColumnNames().contains(ordering.getColumnName())) {
        throw new IllegalArgumentException(
            "The specified ordering column is not found. Invalid ordering: "
                + ordering
                + ", Operation: "
                + scan);
      }
    }
  }

  private void checkConjunctions(ScanAll scan, TableMetadata metadata) {
    for (Conjunction conjunction : scan.getConjunctions()) {
      for (ConditionalExpression condition : conjunction.getConditions()) {
        boolean isValid;
        if (condition.getOperator() == Operator.IS_NULL
            || condition.getOperator() == Operator.IS_NOT_NULL) {
          // the value must be null if the operator is 'is null' or `is not null`
          isValid =
              new ColumnChecker(metadata, false, true, false, false).check(condition.getColumn());
        } else {
          // Otherwise, the value must not be null
          isValid =
              new ColumnChecker(metadata, true, false, false, false).check(condition.getColumn());
        }
        if (!isValid) {
          throw new IllegalArgumentException(
              "The condition is not properly specified. Operation: " + scan);
        }
      }
    }
  }

  public void check(Put put) throws ExecutionException {
    TableMetadata metadata = getTableMetadata(put);
    checkPrimaryKey(put, metadata);
    checkColumnsInPut(put, metadata);
    checkCondition(put, metadata);
  }

  public void check(Delete delete) throws ExecutionException {
    TableMetadata metadata = getTableMetadata(delete);
    checkPrimaryKey(delete, metadata);
    checkCondition(delete, metadata);
  }

  protected TableMetadata getTableMetadata(Operation operation) throws ExecutionException {
    TableMetadata metadata = tableMetadataManager.getTableMetadata(operation);
    if (metadata == null) {
      throw new IllegalArgumentException(
          "The specified table is not found: " + operation.forFullTableName().get());
    }
    return metadata;
  }

  private void checkColumnsInPut(Put put, TableMetadata metadata) {
    for (Column<?> column : put.getColumns().values()) {
      if (!new ColumnChecker(metadata, false, false, false, true).check(column)) {
        throw new IllegalArgumentException(
            "The column value is not properly specified. Invalid column: "
                + column
                + ", Operation: "
                + put);
      }
    }
  }

  private void checkCondition(Mutation mutation, TableMetadata metadata) {
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

  public void check(List<? extends Mutation> mutations) throws ExecutionException {
    if (mutations.isEmpty()) {
      throw new IllegalArgumentException("The mutations are empty");
    }

    Mutation first = mutations.get(0);
    for (Mutation mutation : mutations) {
      if (!mutation.forNamespace().equals(first.forNamespace())
          || !mutation.forTable().equals(first.forTable())
          || !mutation.getPartitionKey().equals(first.getPartitionKey())) {
        throw new IllegalArgumentException(
            "Mutations that span multi-partition are not supported. Mutations: " + mutations);
      }
    }

    for (Mutation mutation : mutations) {
      if (mutation instanceof Put) {
        check((Put) mutation);
      } else {
        assert mutation instanceof Delete;
        check((Delete) mutation);
      }
    }
  }

  private void checkPrimaryKey(Operation operation, TableMetadata metadata) {
    checkPartitionKey(operation, metadata);
    checkClusteringKey(operation, metadata);
  }

  private void checkPartitionKey(Operation operation, TableMetadata metadata) {
    if (!checkKey(operation.getPartitionKey(), metadata.getPartitionKeyNames(), false, metadata)) {
      throw new IllegalArgumentException(
          "The partition key is not properly specified. Operation: " + operation);
    }
  }

  private void checkClusteringKey(Operation operation, TableMetadata metadata) {
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

  private boolean checkKey(
      Key key, LinkedHashSet<String> keyNames, boolean allowPartial, TableMetadata metadata) {
    if (!allowPartial) {
      if (key.size() != keyNames.size()) {
        return false;
      }
    } else {
      if (key.size() > keyNames.size()) {
        return false;
      }
    }

    if (key.size() == 0) {
      return false;
    }

    Iterator<String> iterator = keyNames.iterator();
    for (Column<?> column : key.getColumns()) {
      if (column == null) {
        return false;
      }
      String keyName = iterator.next();

      if (!keyName.equals(column.getName())) {
        return false;
      }

      if (!new ColumnChecker(metadata, true, false, true, false).check(column)) {
        return false;
      }
    }

    return true;
  }
}
