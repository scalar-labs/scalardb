package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.api.ConditionalExpression.Operator;
import static com.scalar.db.transaction.consensuscommit.Attribute.ID;
import static com.scalar.db.transaction.consensuscommit.Attribute.VERSION;
import static com.scalar.db.transaction.consensuscommit.Attribute.toIdValue;
import static com.scalar.db.transaction.consensuscommit.Attribute.toVersionValue;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.TransactionState;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class PrepareMutationComposer extends AbstractMutationComposer {

  public PrepareMutationComposer(String id) {
    super(id);
  }

  @VisibleForTesting
  PrepareMutationComposer(String id, List<Mutation> mutations, long current) {
    super(id, mutations, current);
  }

  @Override
  public void add(Operation base, TransactionResult result) {
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

  private void add(Put base, TransactionResult result) {
    Put put =
        new Put(base.getPartitionKey(), getClusteringKey(base, result).orElse(null))
            .forNamespace(base.forNamespace().get())
            .forTable(base.forTable().get())
            .withConsistency(Consistency.LINEARIZABLE);

    List<Value<?>> values = new ArrayList<>();
    values.add(Attribute.toIdValue(id));
    values.add(Attribute.toStateValue(TransactionState.PREPARED));
    values.add(Attribute.toPreparedAtValue(current));
    values.addAll(base.getValues().values());

    if (result != null) { // overwrite existing record
      values.addAll(createBeforeValues(base, result));
      int version = result.getVersion();
      values.add(Attribute.toVersionValue(version + 1));

      // check if the record is not interrupted by other conflicting transactions
      put.withCondition(
          new PutIf(
              new ConditionalExpression(VERSION, toVersionValue(version), Operator.EQ),
              new ConditionalExpression(ID, toIdValue(result.getId()), Operator.EQ)));
    } else { // initial record
      values.add(Attribute.toVersionValue(1));

      // check if the record is not created by other conflicting transactions
      put.withCondition(new PutIfNotExists());
    }

    put.withValues(values);
    mutations.add(put);
  }

  private void add(Delete base, TransactionResult result) {
    if (result == null) {
      throw new IllegalArgumentException(
          "the record to be deleted must be existing and read beforehand");
    }

    Put put =
        new Put(base.getPartitionKey(), getClusteringKey(base, result).orElse(null))
            .forNamespace(base.forNamespace().get())
            .forTable(base.forTable().get())
            .withConsistency(Consistency.LINEARIZABLE);

    List<Value<?>> values = new ArrayList<>();
    values.add(Attribute.toIdValue(id));
    values.add(Attribute.toStateValue(TransactionState.DELETED));
    values.add(Attribute.toPreparedAtValue(current));
    values.addAll(createBeforeValues(base, result));
    int version = result.getVersion();
    values.add(Attribute.toVersionValue(version + 1));

    // check if the record is not interrupted by other conflicting transactions
    put.withCondition(
        new PutIf(
            new ConditionalExpression(VERSION, toVersionValue(version), Operator.EQ),
            new ConditionalExpression(ID, toIdValue(result.getId()), Operator.EQ)));

    put.withValues(values);
    mutations.add(put);
  }

  // This prepares a record that was read but didn't exist to avoid anti-dependency for the record.
  // This is only called when Serializable with Extra-write strategy is enabled.
  private void add(Get base) {
    Put put =
        new Put(base.getPartitionKey(), getClusteringKey(base, null).orElse(null))
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

  private List<Value<?>> createBeforeValues(Mutation base, TransactionResult result) {
    Key partitionKey = base.getPartitionKey();
    Optional<Key> clusteringKey = getClusteringKey(base, result);

    List<Value<?>> values = new ArrayList<>();
    result
        .getValues()
        .values()
        .forEach(
            v -> {
              if (isBeforeRequired(v, partitionKey, clusteringKey)) {
                values.add(v.copyWith(Attribute.BEFORE_PREFIX + v.getName()));
              }
            });
    return values;
  }

  private boolean isBeforeRequired(Value<?> value, Key primary, Optional<Key> clustering) {
    return !value.getName().startsWith(Attribute.BEFORE_PREFIX)
        && !isValueInKeys(value, primary, clustering);
  }

  private boolean isValueInKeys(Value<?> value, Key primary, Optional<Key> clustering) {
    for (Value<?> v : primary) {
      if (v.equals(value)) {
        return true;
      }
    }

    if (!clustering.isPresent()) {
      return false;
    }

    for (Value<?> v : clustering.get()) {
      if (v.equals(value)) {
        return true;
      }
    }
    return false;
  }
}
