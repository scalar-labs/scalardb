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
import com.scalar.db.api.StorageInfo;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.CoreError;
import com.scalar.db.common.StorageInfoProvider;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import com.scalar.db.util.ScalarDbUtils;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;

/** A checker for the operations for the storage abstraction. */
@ThreadSafe
public class OperationChecker {

  private final DatabaseConfig config;
  private final TableMetadataManager tableMetadataManager;
  private final StorageInfoProvider storageInfoProvider;

  public OperationChecker(
      DatabaseConfig config,
      TableMetadataManager tableMetadataManager,
      StorageInfoProvider storageInfoProvider) {
    this.config = config;
    this.tableMetadataManager = tableMetadataManager;
    this.storageInfoProvider = storageInfoProvider;
  }

  public void check(Get get) throws ExecutionException {
    TableMetadata metadata = getTableMetadata(get);

    checkProjections(get, metadata);

    checkConjunctions(get, metadata);

    if (ScalarDbUtils.isSecondaryIndexSpecified(get, metadata)) {
      if (get.getPartitionKey().size() != 1) {
        throw new IllegalArgumentException(
            CoreError.OPERATION_CHECK_ERROR_INDEX_ONLY_SINGLE_COLUMN_INDEX_SUPPORTED.buildMessage(
                get));
      }

      String name = get.getPartitionKey().getColumns().get(0).getName();
      if (!metadata.getSecondaryIndexNames().contains(name)) {
        throw new IllegalArgumentException(
            CoreError.OPERATION_CHECK_ERROR_INDEX_NON_INDEXED_COLUMN_SPECIFIED.buildMessage(get));
      }

      if (!new ColumnChecker(metadata, true, false, false, false)
          .check(get.getPartitionKey().getColumns().get(0))) {
        throw new IllegalArgumentException(
            CoreError.OPERATION_CHECK_ERROR_INDEX_INDEX_KEY_NOT_PROPERLY_SPECIFIED.buildMessage(
                get));
      }

      // The following check is not needed when we use GetWithIndex. But we need to keep it for
      // backward compatibility. We will remove it in release 5.0.0.
      if (get.getClusteringKey().isPresent()) {
        throw new IllegalArgumentException(
            CoreError.OPERATION_CHECK_ERROR_INDEX_CLUSTERING_KEY_SPECIFIED.buildMessage(get));
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

    checkConjunctions(scan, metadata);

    if (ScalarDbUtils.isSecondaryIndexSpecified(scan, metadata)) {
      if (scan.getPartitionKey().size() != 1) {
        throw new IllegalArgumentException(
            CoreError.OPERATION_CHECK_ERROR_INDEX_ONLY_SINGLE_COLUMN_INDEX_SUPPORTED.buildMessage(
                scan));
      }

      String name = scan.getPartitionKey().getColumns().get(0).getName();
      if (!metadata.getSecondaryIndexNames().contains(name)) {
        throw new IllegalArgumentException(
            CoreError.OPERATION_CHECK_ERROR_INDEX_NON_INDEXED_COLUMN_SPECIFIED.buildMessage(scan));
      }

      if (!new ColumnChecker(metadata, true, false, false, false)
          .check(scan.getPartitionKey().getColumns().get(0))) {
        throw new IllegalArgumentException(
            CoreError.OPERATION_CHECK_ERROR_INDEX_INDEX_KEY_NOT_PROPERLY_SPECIFIED.buildMessage(
                scan));
      }

      // The following checks are not needed when we use ScanWithIndex. But we need to keep them for
      // backward compatibility. We will remove them in release 5.0.0.
      if (scan.getStartClusteringKey().isPresent() || scan.getEndClusteringKey().isPresent()) {
        throw new IllegalArgumentException(
            CoreError.OPERATION_CHECK_ERROR_INDEX_CLUSTERING_KEY_SPECIFIED.buildMessage(scan));
      }

      if (!scan.getOrderings().isEmpty()) {
        throw new IllegalArgumentException(
            CoreError.OPERATION_CHECK_ERROR_INDEX_ORDERING_SPECIFIED.buildMessage(scan));
      }
      return;
    }

    checkPartitionKey(scan, metadata);

    checkClusteringKeys(scan, metadata);

    if (scan.getLimit() < 0) {
      throw new IllegalArgumentException(CoreError.OPERATION_CHECK_ERROR_LIMIT.buildMessage(scan));
    }

    checkOrderings(scan, metadata);
  }

  private void check(ScanAll scanAll) throws ExecutionException {
    if (!config.isCrossPartitionScanEnabled()) {
      throw new IllegalArgumentException(
          CoreError.OPERATION_CHECK_ERROR_CROSS_PARTITION_SCAN.buildMessage(scanAll));
    }

    TableMetadata metadata = getTableMetadata(scanAll);

    checkProjections(scanAll, metadata);

    if (scanAll.getLimit() < 0) {
      throw new IllegalArgumentException(
          CoreError.OPERATION_CHECK_ERROR_LIMIT.buildMessage(scanAll));
    }

    if (!config.isCrossPartitionScanOrderingEnabled() && !scanAll.getOrderings().isEmpty()) {
      throw new IllegalArgumentException(
          CoreError.OPERATION_CHECK_ERROR_CROSS_PARTITION_SCAN_ORDERING.buildMessage(scanAll));
    }
    checkOrderings(scanAll, metadata);

    if (!config.isCrossPartitionScanFilteringEnabled() && !scanAll.getConjunctions().isEmpty()) {
      throw new IllegalArgumentException(
          CoreError.OPERATION_CHECK_ERROR_CROSS_PARTITION_SCAN_FILTERING.buildMessage(scanAll));
    }
    checkConjunctions(scanAll, metadata);
  }

  private void checkProjections(Selection selection, TableMetadata metadata) {
    for (String projection : selection.getProjections()) {
      if (!metadata.getColumnNames().contains(projection)) {
        throw new IllegalArgumentException(
            CoreError.OPERATION_CHECK_ERROR_PROJECTION.buildMessage(projection, selection));
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
          () -> CoreError.OPERATION_CHECK_ERROR_CLUSTERING_KEY_BOUNDARY.buildMessage(scan);

      if (startClusteringKey.size() != endClusteringKey.size()) {
        throw new IllegalArgumentException(message.get());
      }

      for (int i = 0; i < startClusteringKey.size() - 1; i++) {
        Column<?> startValue = startClusteringKey.getColumns().get(i);
        Column<?> endValue = endClusteringKey.getColumns().get(i);
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
                    CoreError.OPERATION_CHECK_ERROR_START_CLUSTERING_KEY.buildMessage(scan));
              }
            });
  }

  private void checkEndClusteringKey(Scan scan, TableMetadata metadata) {
    scan.getEndClusteringKey()
        .ifPresent(
            ckey -> {
              if (!checkKey(ckey, metadata.getClusteringKeyNames(), true, metadata)) {
                throw new IllegalArgumentException(
                    CoreError.OPERATION_CHECK_ERROR_END_CLUSTERING_KEY.buildMessage(scan));
              }
            });
  }

  private void checkOrderings(Scan scan, TableMetadata metadata) {
    List<Scan.Ordering> orderings = scan.getOrderings();
    if (orderings.isEmpty()) {
      return;
    }

    Supplier<String> message =
        () -> CoreError.OPERATION_CHECK_ERROR_ORDERING_NOT_PROPERLY_SPECIFIED.buildMessage(scan);

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

  private void checkOrderings(ScanAll scanAll, TableMetadata metadata) {
    for (Scan.Ordering ordering : scanAll.getOrderings()) {
      if (!metadata.getColumnNames().contains(ordering.getColumnName())) {
        throw new IllegalArgumentException(
            CoreError.OPERATION_CHECK_ERROR_ORDERING_COLUMN_NOT_FOUND.buildMessage(
                ordering, scanAll));
      }
    }
  }

  private void checkConjunctions(Selection selection, TableMetadata metadata) {
    for (Conjunction conjunction : selection.getConjunctions()) {
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
              CoreError.OPERATION_CHECK_ERROR_CONDITION.buildMessage(selection));
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
      assert operation.forFullTableName().isPresent();
      throw new IllegalArgumentException(
          CoreError.TABLE_NOT_FOUND.buildMessage(operation.forFullTableName().get()));
    }
    return metadata;
  }

  private void checkColumnsInPut(Put put, TableMetadata metadata) {
    for (Column<?> column : put.getColumns().values()) {
      if (!new ColumnChecker(metadata, false, false, false, true).check(column)) {
        throw new IllegalArgumentException(
            CoreError.OPERATION_CHECK_ERROR_INVALID_COLUMN.buildMessage(column, put));
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
                    CoreError.OPERATION_CHECK_ERROR_CONDITION.buildMessage(mutation));
              }
            });
  }

  public void check(List<? extends Mutation> mutations) throws ExecutionException {
    if (mutations.isEmpty()) {
      throw new IllegalArgumentException(CoreError.EMPTY_MUTATIONS_SPECIFIED.buildMessage());
    }

    Mutation first = mutations.get(0);
    assert first.forNamespace().isPresent();
    StorageInfo storageInfoForFirst =
        storageInfoProvider.getStorageInfo(first.forNamespace().get());

    for (Mutation mutation : mutations) {
      // Check if each mutation is Put or Delete
      checkMutationType(mutation);

      // Check if the mutations are within the atomicity unit of the storage
      if (isOutOfAtomicityUnit(first, storageInfoForFirst, mutation)) {
        throw new IllegalArgumentException(
            getErrorMessageForOutOfAtomicityUnit(storageInfoForFirst, mutations));
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

  private boolean isOutOfAtomicityUnit(
      Mutation mutation1, StorageInfo storageInfo1, Mutation mutation2) throws ExecutionException {
    assert mutation1.forNamespace().isPresent()
        && mutation1.forTable().isPresent()
        && mutation2.forNamespace().isPresent()
        && mutation2.forTable().isPresent();

    switch (storageInfo1.getMutationAtomicityUnit()) {
      case RECORD:
        if (!mutation1.getClusteringKey().equals(mutation2.getClusteringKey())) {
          return true; // Different clustering keys
        }
        // Fall through
      case PARTITION:
        if (!mutation1.getPartitionKey().equals(mutation2.getPartitionKey())) {
          return true; // Different partition keys
        }
        // Fall through
      case TABLE:
        if (!mutation1.forTable().equals(mutation2.forTable())) {
          return true; // Different tables
        }
        // Fall through
      case NAMESPACE:
        if (!mutation1.forNamespace().equals(mutation2.forNamespace())) {
          return true; // Different namespaces
        }
        break;
      case STORAGE:
        StorageInfo storageInfo2 =
            storageInfoProvider.getStorageInfo(mutation2.forNamespace().get());
        if (!storageInfo1.getStorageName().equals(storageInfo2.getStorageName())) {
          return true; // Different storage names
        }
        break;
      default:
        throw new AssertionError(
            "Unknown mutation atomicity unit: " + storageInfo1.getMutationAtomicityUnit());
    }

    return false;
  }

  private String getErrorMessageForOutOfAtomicityUnit(
      StorageInfo storageInfo, List<? extends Mutation> mutations) {
    switch (storageInfo.getMutationAtomicityUnit()) {
      case RECORD:
        return CoreError.OPERATION_CHECK_ERROR_MULTI_RECORD_MUTATION.buildMessage(
            storageInfo.getStorageName(), mutations);
      case PARTITION:
        return CoreError.OPERATION_CHECK_ERROR_MULTI_PARTITION_MUTATION.buildMessage(
            storageInfo.getStorageName(), mutations);
      case TABLE:
        return CoreError.OPERATION_CHECK_ERROR_MULTI_TABLE_MUTATION.buildMessage(
            storageInfo.getStorageName(), mutations);
      case NAMESPACE:
        return CoreError.OPERATION_CHECK_ERROR_MULTI_NAMESPACE_MUTATION.buildMessage(
            storageInfo.getStorageName(), mutations);
      case STORAGE:
        return CoreError.OPERATION_CHECK_ERROR_MULTI_STORAGE_MUTATION.buildMessage(mutations);
      default:
        throw new AssertionError(
            "Unknown mutation atomicity unit: " + storageInfo.getMutationAtomicityUnit());
    }
  }

  private void checkMutationType(Mutation mutation) {
    if (!(mutation instanceof Put) && !(mutation instanceof Delete)) {
      throw new IllegalArgumentException(
          CoreError.OPERATION_CHECK_ERROR_UNSUPPORTED_MUTATION_TYPE.buildMessage(mutation));
    }
  }

  private void checkPrimaryKey(Operation operation, TableMetadata metadata) {
    checkPartitionKey(operation, metadata);
    checkClusteringKey(operation, metadata);
  }

  private void checkPartitionKey(Operation operation, TableMetadata metadata) {
    if (!checkKey(operation.getPartitionKey(), metadata.getPartitionKeyNames(), false, metadata)) {
      throw new IllegalArgumentException(
          CoreError.OPERATION_CHECK_ERROR_PARTITION_KEY.buildMessage(operation));
    }
  }

  private void checkClusteringKey(Operation operation, TableMetadata metadata) {
    Supplier<String> message =
        () -> CoreError.OPERATION_CHECK_ERROR_CLUSTERING_KEY.buildMessage(operation);

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
