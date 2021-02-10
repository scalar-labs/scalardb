package com.scalar.db.storage.jdbc.checker;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.exception.storage.MultiPartitionException;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.storage.jdbc.metadata.JdbcTableMetadata;
import com.scalar.db.storage.jdbc.metadata.TableMetadataManager;

import javax.annotation.concurrent.ThreadSafe;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A class to check operations and throw exceptions if operations have any problems
 *
 * @author Toshihiro Suzuki
 */
@ThreadSafe
public class OperationChecker {

  private final TableMetadataManager tableMetadataManager;

  public OperationChecker(TableMetadataManager tableMetadataManager) {
    this.tableMetadataManager = Objects.requireNonNull(tableMetadataManager);
  }

  public void check(Get get) throws SQLException {
    JdbcTableMetadata tableMetadata = getTableMetadata(get);
    checkProjections(tableMetadata, get.getProjections());
    boolean isIndexUsed = checkPartitionKey(tableMetadata, get.getPartitionKey(), true);
    if (isIndexUsed) {
      if (get.getClusteringKey().isPresent()) {
        throw new IllegalArgumentException(
            "the clusteringKey should not be specified when using a index");
      }
    } else {
      if (get.getClusteringKey().isPresent()) {
        checkClusteringKey(tableMetadata, get.getClusteringKey().get(), false);
      } else {
        if (tableMetadata.getClusteringKeys().size() > 0) {
          throw new IllegalArgumentException("the clusteringKey should be specified");
        }
      }
    }
  }

  public void check(Scan scan) throws SQLException {
    JdbcTableMetadata tableMetadata = getTableMetadata(scan);
    checkProjections(tableMetadata, scan.getProjections());

    boolean isIndexUsed = checkPartitionKey(tableMetadata, scan.getPartitionKey(), true);
    if (isIndexUsed) {
      if (scan.getStartClusteringKey().isPresent() || scan.getEndClusteringKey().isPresent()) {
        throw new IllegalArgumentException(
            "the clusteringKey should not be specified when using a index");
      }
    } else {
      scan.getStartClusteringKey()
          .ifPresent(
              startClusteringKey -> checkClusteringKey(tableMetadata, startClusteringKey, true));

      scan.getEndClusteringKey()
          .ifPresent(endClusteringKey -> checkClusteringKey(tableMetadata, endClusteringKey, true));

      if (scan.getStartClusteringKey().isPresent() && scan.getEndClusteringKey().isPresent()) {
        checkClusteringKeyRange(
            scan.getStartClusteringKey().get(), scan.getEndClusteringKey().get());
      }
    }

    if (scan.getLimit() < 0) {
      throw new IllegalArgumentException("the limit should not be negative");
    }

    checkOrderings(tableMetadata, scan.getOrderings(), isIndexUsed);
  }

  public void check(Put put) throws SQLException {
    JdbcTableMetadata tableMetadata = getTableMetadata(put);
    checkPartitionKey(tableMetadata, put.getPartitionKey(), false);
    if (put.getClusteringKey().isPresent()) {
      checkClusteringKey(tableMetadata, put.getClusteringKey().get(), false);
    } else {
      if (tableMetadata.getClusteringKeys().size() > 0) {
        throw new IllegalArgumentException("the clusteringKey should be specified");
      }
    }
    checkValues(tableMetadata, put.getValues());

    put.getCondition().ifPresent(condition -> checkCondition(tableMetadata, condition, true));
  }

  public void check(Delete delete) throws SQLException {
    JdbcTableMetadata tableMetadata = getTableMetadata(delete);
    checkPartitionKey(tableMetadata, delete.getPartitionKey(), false);
    if (delete.getClusteringKey().isPresent()) {
      checkClusteringKey(tableMetadata, delete.getClusteringKey().get(), false);
    } else {
      if (tableMetadata.getClusteringKeys().size() > 0) {
        throw new IllegalArgumentException("the clusteringKey should be specified");
      }
    }
    delete.getCondition().ifPresent(condition -> checkCondition(tableMetadata, condition, false));
  }

  public void check(List<? extends Mutation> mutations, boolean allowMultiPartitions) {
    if (mutations.isEmpty()) {
      throw new IllegalArgumentException("the mutations are empty");
    }

    if (allowMultiPartitions) {
      return;
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

  private JdbcTableMetadata getTableMetadata(Operation operation) throws SQLException {
    JdbcTableMetadata tableMetadata =
        tableMetadataManager.getTableMetadata(operation.forFullTableName().get());
    if (tableMetadata == null) {
      throw new IllegalArgumentException("the table is not found: " + operation.forFullTableName());
    }
    return tableMetadata;
  }

  private void checkProjections(JdbcTableMetadata tableMetadata, List<String> projections) {
    for (String projection : projections) {
      if (!tableMetadata.columnExists(projection)) {
        throw new IllegalArgumentException(
            "the projection is not found in the table metadata: " + projection);
      }
    }
  }

  /** @return whether or not an index is used */
  private boolean checkPartitionKey(
      JdbcTableMetadata tableMetadata, Key partitionKey, boolean allowUsingIndex) {
    if (allowUsingIndex && partitionKey.get().size() == 1) {
      Value value = partitionKey.get().get(0);
      if (tableMetadata.isIndexedColumn(value.getName())
          && new ColumnDataTypeChecker(tableMetadata).check(value)) {
        // We use this index
        return true;
      }
    }

    if (!checkKey(tableMetadata, tableMetadata.getPartitionKeys(), partitionKey, false)) {
      throw new IllegalArgumentException("the partitionKey is invalid: " + partitionKey);
    }

    return false;
  }

  private void checkClusteringKey(
      JdbcTableMetadata tableMetadata, Key clusteringKey, boolean allowPartial) {
    if (!checkKey(tableMetadata, tableMetadata.getClusteringKeys(), clusteringKey, allowPartial)) {
      throw new IllegalArgumentException("the clusteringKey is invalid: " + clusteringKey);
    }
  }

  private boolean checkKey(
      JdbcTableMetadata tableMetadata, List<String> keys, Key key, boolean allowPartial) {
    List<Value> values = new ArrayList<>(key.get());

    if (!allowPartial) {
      if (values.size() != keys.size()) {
        return false;
      }
    } else {
      if (values.size() > keys.size()) {
        return false;
      }
    }

    for (int i = 0; i < values.size(); i++) {
      String k = keys.get(i);
      Value value = values.get(i);

      if (!k.equals(value.getName())) {
        return false;
      }

      if (!new ColumnDataTypeChecker(tableMetadata).check(value)) {
        return false;
      }
    }

    return true;
  }

  private void checkClusteringKeyRange(Key startClusteringKey, Key endClusteringKey) {
    if (startClusteringKey.size() != endClusteringKey.size()) {
      throw new IllegalArgumentException("the clustering keys are invalid");
    }

    for (int i = 0; i < startClusteringKey.size() - 1; i++) {
      Value startValue = startClusteringKey.get().get(i);
      Value endValue = endClusteringKey.get().get(i);
      if (!startValue.equals(endValue)) {
        throw new IllegalArgumentException("the clustering keys are invalid");
      }
    }
  }

  private void checkValues(JdbcTableMetadata tableMetadata, Map<String, Value> values) {
    for (Map.Entry<String, Value> entry : values.entrySet()) {
      if (!new ColumnDataTypeChecker(tableMetadata).check(entry.getValue())) {
        throw new IllegalArgumentException("the type of the value is invalid: " + entry.getKey());
      }
    }
  }

  private void checkCondition(
      JdbcTableMetadata tableMetadata, MutationCondition condition, boolean isPut) {
    if (!new ConditionChecker(tableMetadata).check(condition, isPut)) {
      throw new IllegalArgumentException("the condition is invalid: " + condition);
    }
  }

  private void checkOrderings(
      JdbcTableMetadata tableMetadata, List<Scan.Ordering> orderings, boolean isIndexUsed) {
    if (orderings.isEmpty()) {
      return;
    }

    if (isIndexUsed) {
      throw new IllegalArgumentException(
          "The ordering should not be specified when using an index");
    }

    List<String> clusteringKeys = tableMetadata.getClusteringKeys();

    if (orderings.size() > clusteringKeys.size()) {
      throw new IllegalArgumentException("invalid orderings: " + orderings);
    }

    Boolean reverse = null;
    for (int i = 0; i < orderings.size(); i++) {
      Scan.Ordering ordering = orderings.get(i);
      String clusteringKeyName = clusteringKeys.get(i);
      if (!ordering.getName().equals(clusteringKeyName)) {
        throw new IllegalArgumentException("invalid orderings: " + orderings);
      }

      boolean rightOrder =
          ordering.getOrder() != tableMetadata.getClusteringKeyOrder(ordering.getName());
      if (reverse == null) {
        reverse = rightOrder;
      } else {
        if (reverse != rightOrder) {
          throw new IllegalArgumentException("invalid orderings: " + orderings);
        }
      }
    }
  }
}
