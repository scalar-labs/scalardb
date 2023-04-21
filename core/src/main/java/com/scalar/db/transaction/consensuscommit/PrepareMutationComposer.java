package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.transaction.consensuscommit.Attribute.ID;
import static com.scalar.db.transaction.consensuscommit.Attribute.VERSION;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.TransactionState;
import com.scalar.db.io.Column;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class PrepareMutationComposer extends AbstractMutationComposer {

  public PrepareMutationComposer(String id) {
    super(id);
  }

  @VisibleForTesting
  PrepareMutationComposer(String id, long current) {
    super(id, current);
  }

  @Override
  public void add(Operation base, @Nullable TransactionResult result) {
    if (base instanceof Put) {
      add((Put) base, result);
    } else if (base instanceof Delete) {
      add((Delete) base, result);
    } else if (base instanceof Get) {
      add((Get) base);
    } else {
      throw new IllegalArgumentException("PrepareMutationComposer.add only accepts Put or Delete");
    }
  }

  private void add(Put base, @Nullable TransactionResult result) {
    PutBuilder.Buildable putBuilder =
        Put.newBuilder()
            .namespace(base.forNamespace().get())
            .table(base.forTable().get())
            .partitionKey(base.getPartitionKey())
            .consistency(Consistency.LINEARIZABLE);
    base.getClusteringKey().ifPresent(putBuilder::clusteringKey);
    base.getColumns().values().forEach(putBuilder::value);

    putBuilder.textValue(Attribute.ID, id);
    putBuilder.intValue(Attribute.STATE, TransactionState.PREPARED.get());
    putBuilder.bigIntValue(Attribute.PREPARED_AT, current);

    if (result != null) { // overwrite existing record
      createBeforeColumns(base, result).forEach(putBuilder::value);
      int version = result.getVersion();
      putBuilder.intValue(Attribute.VERSION, version + 1);

      // check if the record is not interrupted by other conflicting transactions
      if (result.getId() == null) {
        // record is deemed-commit state
        putBuilder.condition(
            ConditionBuilder.putIf(ConditionBuilder.column(ID).isNullText())
                .and(ConditionBuilder.column(VERSION).isNullInt())
                .build());
      } else {
        putBuilder.condition(
            ConditionBuilder.putIf(ConditionBuilder.column(ID).isEqualToText(result.getId()))
                .and(ConditionBuilder.column(VERSION).isEqualToInt(version))
                .build());
      }
    } else { // initial record
      putBuilder.intValue(Attribute.VERSION, 1);

      // check if the record is not created by other conflicting transactions
      putBuilder.condition(ConditionBuilder.putIfNotExists());
    }

    mutations.add(putBuilder.build());
  }

  private void add(Delete base, @Nullable TransactionResult result) {
    PutBuilder.Buildable putBuilder =
        Put.newBuilder()
            .namespace(base.forNamespace().get())
            .table(base.forTable().get())
            .partitionKey(base.getPartitionKey())
            .consistency(Consistency.LINEARIZABLE);
    base.getClusteringKey().ifPresent(putBuilder::clusteringKey);

    putBuilder.textValue(Attribute.ID, id);
    putBuilder.intValue(Attribute.STATE, TransactionState.DELETED.get());
    putBuilder.bigIntValue(Attribute.PREPARED_AT, current);

    if (result != null) {
      createBeforeColumns(base, result).forEach(putBuilder::value);
      int version = result.getVersion();
      putBuilder.intValue(Attribute.VERSION, version + 1);

      // check if the record is not interrupted by other conflicting transactions
      if (result.getId() == null) {
        // record is deemed-commit state
        putBuilder.condition(
            ConditionBuilder.putIf(ConditionBuilder.column(ID).isNullText())
                .and(ConditionBuilder.column(VERSION).isNullInt())
                .build());
      } else {
        putBuilder.condition(
            ConditionBuilder.putIf(ConditionBuilder.column(ID).isEqualToText(result.getId()))
                .and(ConditionBuilder.column(VERSION).isEqualToInt(version))
                .build());
      }
    } else {
      putBuilder.intValue(Attribute.VERSION, 1);

      // check if the record is not created by other conflicting transactions
      putBuilder.condition(ConditionBuilder.putIfNotExists());
    }

    mutations.add(putBuilder.build());
  }

  // This prepares a record that was read but didn't exist to avoid anti-dependency for the record.
  // This is only called when Serializable with Extra-write strategy is enabled.
  private void add(Get base) {
    Put put =
        new Put(base.getPartitionKey(), base.getClusteringKey().orElse(null))
            .forNamespace(base.forNamespace().get())
            .forTable(base.forTable().get())
            .withConsistency(Consistency.LINEARIZABLE);

    List<Value<?>> values = new ArrayList<>();
    values.add(Attribute.toIdValue(id));
    values.add(Attribute.toStateValue(TransactionState.DELETED));
    values.add(Attribute.toPreparedAtValue(current));
    values.add(Attribute.toVersionValue(1));

    // check if the record is not interrupted by other conflicting transactions
    put.withCondition(new PutIfNotExists());

    put.withValues(values);
    mutations.add(put);
  }

  private List<Column<?>> createBeforeColumns(Mutation base, TransactionResult result) {
    List<Column<?>> columns = new ArrayList<>();
    result
        .getColumns()
        .values()
        .forEach(
            column -> {
              if (isBeforeRequired(column, base.getPartitionKey(), base.getClusteringKey())) {
                if (column.getName().equals(Attribute.VERSION) && column.hasNullValue()) {
                  // A prepare-state record with NULLs for both before_id and before_version will be
                  // deleted as an initial record in a rollback situation. To avoid this and roll
                  // back to a record with a NULL version (i.e., regarded as committed) correctly,
                  // we need to use version 0 rather than NULL for before_version.
                  columns.add(IntColumn.of(Attribute.BEFORE_VERSION, 0));
                } else {
                  columns.add(column.copyWith(Attribute.BEFORE_PREFIX + column.getName()));
                }
              }
            });
    return columns;
  }

  private boolean isBeforeRequired(Column<?> column, Key primary, Optional<Key> clustering) {
    return !column.getName().startsWith(Attribute.BEFORE_PREFIX)
        && !isColumnInKeys(column, primary, clustering);
  }

  private boolean isColumnInKeys(Column<?> column, Key primary, Optional<Key> clustering) {
    for (Column<?> c : primary.getColumns()) {
      if (c.equals(column)) {
        return true;
      }
    }

    if (!clustering.isPresent()) {
      return false;
    }

    for (Column<?> c : clustering.get().getColumns()) {
      if (c.equals(column)) {
        return true;
      }
    }
    return false;
  }
}
