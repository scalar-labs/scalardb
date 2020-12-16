package com.scalar.db.storage.jdbc;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.MutationConditionVisitor;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.Scan;
import com.scalar.db.exception.storage.MultiPartitionException;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import com.scalar.db.io.ValueVisitor;
import com.scalar.db.storage.jdbc.metadata.DataType;
import com.scalar.db.storage.jdbc.metadata.TableMetadata;
import com.scalar.db.storage.jdbc.metadata.TableMetadataManager;

import javax.annotation.Nullable;
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

  private static class ColumnDataTypeChecker implements ValueVisitor {
    private final TableMetadata tableMetadata;
    private String name;
    private boolean okay;

    public ColumnDataTypeChecker(TableMetadata tableMetadata) {
      this.tableMetadata = tableMetadata;
    }

    private boolean check(Value value) {
      value.accept(this);
      return okay;
    }

    private boolean check(String name, Value value) {
      this.name = name;
      value.accept(this);
      return okay;
    }

    private String getName(Value value) {
      return name != null ? name : value.getName();
    }

    @Override
    public void visit(BooleanValue value) {
      okay = tableMetadata.getDataType(getName(value)) == DataType.BOOLEAN;
    }

    @Override
    public void visit(IntValue value) {
      okay = tableMetadata.getDataType(getName(value)) == DataType.INT;
    }

    @Override
    public void visit(BigIntValue value) {
      okay = tableMetadata.getDataType(getName(value)) == DataType.BIGINT;
    }

    @Override
    public void visit(FloatValue value) {
      okay = tableMetadata.getDataType(getName(value)) == DataType.FLOAT;
    }

    @Override
    public void visit(DoubleValue value) {
      okay = tableMetadata.getDataType(getName(value)) == DataType.DOUBLE;
    }

    @Override
    public void visit(TextValue value) {
      okay = tableMetadata.getDataType(getName(value)) == DataType.TEXT;
    }

    @Override
    public void visit(BlobValue value) {
      okay = tableMetadata.getDataType(getName(value)) == DataType.BLOB;
    }
  }

  private static class ConditionChecker implements MutationConditionVisitor {
    private final TableMetadata tableMetadata;
    private boolean isPut;
    private boolean okay;

    public ConditionChecker(TableMetadata tableMetadata) {
      this.tableMetadata = tableMetadata;
    }

    private boolean check(MutationCondition condition, boolean isPut) {
      this.isPut = isPut;
      condition.accept(this);
      return okay;
    }

    @Override
    public void visit(PutIf condition) {
      if (!isPut) {
        okay = false;
        return;
      }

      // Check the values in the expressions
      for (ConditionalExpression expression : condition.getExpressions()) {
        okay = new ColumnDataTypeChecker(tableMetadata).check(expression.getName(),
          expression.getValue());
        if (!okay) {
          break;
        }
      }
    }

    @Override
    public void visit(PutIfExists condition) {
      okay = isPut;
    }

    @Override
    public void visit(PutIfNotExists condition) {
      okay = isPut;
    }

    @Override
    public void visit(DeleteIf condition) {
      if (isPut) {
        okay = false;
        return;
      }

      // Check the values in the expressions
      for (ConditionalExpression expression : condition.getExpressions()) {
        okay = new ColumnDataTypeChecker(tableMetadata).check(expression.getName(),
          expression.getValue());
        if (!okay) {
          break;
        }
      }
    }

    @Override
    public void visit(DeleteIfExists condition) {
      okay = !isPut;
    }
  }

  private final TableMetadataManager tableMetadataManager;

  public OperationChecker(TableMetadataManager tableMetadataManager) {
    this.tableMetadataManager = Objects.requireNonNull(tableMetadataManager);
  }

  public void checkGet(Table table, List<String> projections, Key partitionKey,
    @Nullable Key clusteringKey) throws SQLException {
    TableMetadata tableMetadata = getTableMetadata(table);
    checkProjections(tableMetadata, projections);
    String indexedColumnToBeUsed = checkPartitionKey(tableMetadata, partitionKey, true);
    if (indexedColumnToBeUsed != null) {
      if (clusteringKey != null) {
        throw new IllegalArgumentException(
          "The clusteringKey should not be specified when using a index");
      }
    } else {
      if (clusteringKey != null) {
        checkClusteringKey(tableMetadata, clusteringKey, false);
      } else {
        if (tableMetadata.getClusteringKeys().size() > 0) {
          throw new IllegalArgumentException("The clusteringKey is null");
        }
      }
    }
  }

  public void checkScan(Table table, List<String> projections, Key partitionKey,
    @Nullable Key startClusteringKey, @Nullable Key endClusteringKey, int limit,
    List<Scan.Ordering> orderings) throws SQLException {
    TableMetadata tableMetadata = getTableMetadata(table);
    checkProjections(tableMetadata, projections);

    String indexedColumnToBeUsed = checkPartitionKey(tableMetadata, partitionKey, true);
    if (indexedColumnToBeUsed != null) {
      if (startClusteringKey != null || endClusteringKey != null) {
        throw new IllegalArgumentException(
          "The clusteringKey should not be specified when using a index");
      }
    } else {
      if (startClusteringKey != null) {
        checkClusteringKey(tableMetadata, startClusteringKey, true);
      }

      if (endClusteringKey != null) {
        checkClusteringKey(tableMetadata, endClusteringKey, true);
      }

      if (startClusteringKey != null && endClusteringKey != null) {
        checkClusteringKeyRange(startClusteringKey, endClusteringKey);
      }
    }

    if (limit < 0) {
      throw new IllegalArgumentException("limit must not be negative");
    }

    checkOrderings(tableMetadata, orderings, indexedColumnToBeUsed);
  }

  public void checkPut(Table table, Key partitionKey, @Nullable Key clusteringKey,
    Map<String, Value> values, @Nullable MutationCondition condition) throws SQLException {
    TableMetadata tableMetadata = getTableMetadata(table);
    checkPartitionKey(tableMetadata, partitionKey, false);
    if (clusteringKey != null) {
      checkClusteringKey(tableMetadata, clusteringKey, false);
    } else {
      if (tableMetadata.getClusteringKeys().size() > 0) {
        throw new IllegalArgumentException("The clusteringKey is null");
      }
    }
    checkValues(tableMetadata, values);
    if (condition != null) {
      checkCondition(tableMetadata, condition, true);
    }
  }

  public void checkDelete(Table table, Key partitionKey, @Nullable Key clusteringKey,
    @Nullable MutationCondition condition) throws SQLException {
    TableMetadata tableMetadata = getTableMetadata(table);
    checkPartitionKey(tableMetadata, partitionKey, false);
    if (clusteringKey != null) {
      checkClusteringKey(tableMetadata, clusteringKey, false);
    } else {
      if (tableMetadata.getClusteringKeys().size() > 0) {
        throw new IllegalArgumentException("The clusteringKey is null");
      }
    }
    if (condition != null) {
      checkCondition(tableMetadata, condition, false);
    }
  }

  public void checkMutate(List<? extends Mutation> mutations) {
    if (mutations.isEmpty()) {
      throw new IllegalArgumentException("mutation is empty");
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

  private TableMetadata getTableMetadata(Table table) throws SQLException {
    TableMetadata tableMetadata = tableMetadataManager.getTableMetadata(table);
    if (tableMetadata == null) {
      throw new IllegalArgumentException("The table is not found: " + table);
    }
    return tableMetadata;
  }

  private void checkProjections(TableMetadata tableMetadata, List<String> projections) {
    for (String projection : projections) {
      if (!tableMetadata.columnExists(projection)) {
        throw new IllegalArgumentException("the projection is not found in the table metadata: "
          + projection);
      }
    }
  }

  /**
   * @return The indexed column name when using the index. Otherwise null
   */
  private String checkPartitionKey(TableMetadata tableMetadata, Key partitionKey,
    boolean allowUsingIndex) {
    if (partitionKey == null) {
      throw new IllegalArgumentException("the partitionKey is null");
    }

    if (!checkKey(tableMetadata, tableMetadata.getPartitionKeys(), partitionKey, false)) {
      if (allowUsingIndex && partitionKey.get().size() == 1) {
        Value value = partitionKey.get().get(0);
        if (tableMetadata.indexedColumn(value.getName()) &&
          new ColumnDataTypeChecker(tableMetadata).check(value)) {
          // We use this index
          return value.getName();
        }
      }
      throw new IllegalArgumentException("The partitionKey is invalid: " + partitionKey);
    }
    return null;
  }

  private void checkClusteringKey(TableMetadata tableMetadata, Key clusteringKey,
    boolean allowPartial) {
    if (!checkKey(tableMetadata, tableMetadata.getClusteringKeys(), clusteringKey, allowPartial)) {
      throw new IllegalArgumentException("The clusteringKey is invalid: " + clusteringKey);
    }
  }

  private boolean checkKey(TableMetadata tableMetadata, List<String> keys, Key key, boolean allowPartial) {
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
      throw new IllegalArgumentException("The clustering keys are invalid");
    }

    for (int i = 0; i < startClusteringKey.size() - 1; i++) {
      Value startValue = startClusteringKey.get().get(i);
      Value endValue = endClusteringKey.get().get(i);
      if (!startValue.equals(endValue)) {
        throw new IllegalArgumentException("The clustering keys are invalid");
      }
    }
  }

  private void checkValues(TableMetadata tableMetadata, Map<String, Value> values) {
    for (Map.Entry<String, Value> entry : values.entrySet()) {
      if (!new ColumnDataTypeChecker(tableMetadata).check(entry.getValue())) {
        throw new IllegalArgumentException("The type of the value is invalid: " + entry.getKey());
      }
    }
  }

  private void checkCondition(TableMetadata tableMetadata, MutationCondition condition,
    boolean isPut) {
    if (!new ConditionChecker(tableMetadata).check(condition, isPut)) {
      throw new IllegalArgumentException("The condition is invalid: " + condition);
    }
  }

  private void checkOrderings(TableMetadata tableMetadata, List<Scan.Ordering> orderings,
    String indexedColumnToBeUsed) {
    if (orderings.isEmpty()) {
      return;
    }

    if (indexedColumnToBeUsed != null) {
      if (orderings.size() != 1 || !orderings.get(0).getName().equals(indexedColumnToBeUsed)) {
        throw new IllegalArgumentException("Invalid orderings: " + orderings);
      }
      return;
    }

    List<String> clusteringKeys = tableMetadata.getClusteringKeys();

    if (orderings.size() > clusteringKeys.size()) {
      throw new IllegalArgumentException("Invalid orderings: " + orderings);
    }

    Boolean reverse = null;
    for (int i = 0; i < orderings.size(); i++) {
      Scan.Ordering ordering = orderings.get(i);
      String clusteringKeyName = clusteringKeys.get(i);
      if (!ordering.getName().equals(clusteringKeyName)) {
        throw new IllegalArgumentException("Invalid orderings: " + orderings);
      }

      boolean rightOrder = ordering.getOrder() !=
        tableMetadata.getClusteringKeyOrder(ordering.getName());
      if (reverse == null) {
        reverse = rightOrder;
      } else {
        if (reverse != rightOrder) {
          throw new IllegalArgumentException("Invalid orderings: " + orderings);
        }
      }
    }
  }
}
