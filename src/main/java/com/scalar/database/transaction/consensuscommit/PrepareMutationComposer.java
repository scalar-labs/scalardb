package com.scalar.database.transaction.consensuscommit;

import static com.scalar.database.api.ConditionalExpression.Operator;
import static com.scalar.database.transaction.consensuscommit.Attribute.*;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.database.api.ConditionalExpression;
import com.scalar.database.api.Consistency;
import com.scalar.database.api.Delete;
import com.scalar.database.api.Mutation;
import com.scalar.database.api.Operation;
import com.scalar.database.api.Put;
import com.scalar.database.api.PutIf;
import com.scalar.database.api.PutIfNotExists;
import com.scalar.database.api.TransactionState;
import com.scalar.database.exception.transaction.InvalidUsageException;
import com.scalar.database.io.Key;
import com.scalar.database.io.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;

/** Prepare mutations in order to commit to {@link com.scalar.database.api.DistributedStorage}. */
@NotThreadSafe
public class PrepareMutationComposer extends AbstractMutationComposer {

  /** Construct a {@code PrepareMutationComposer} with the given id */
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

    List<Value> values = new ArrayList<>();
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
          new PutIf(new ConditionalExpression(VERSION, toVersionValue(version), Operator.EQ)));
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
      throw new InvalidUsageException(
          "the record to be deleted must be existing " + "and read beforehand");
    }

    Put put =
        new Put(base.getPartitionKey(), getClusteringKey(base, result).orElse(null))
            .forNamespace(base.forNamespace().get())
            .forTable(base.forTable().get())
            .withConsistency(Consistency.LINEARIZABLE);

    List<Value> values = new ArrayList<>();
    values.add(Attribute.toIdValue(id));
    values.add(Attribute.toStateValue(TransactionState.DELETED));
    values.add(Attribute.toPreparedAtValue(current));
    values.addAll(createBeforeValues(base, result));
    int version = result.getVersion();
    values.add(Attribute.toVersionValue(version + 1));

    // check if the record is not interrupted by other conflicting transactions
    put.withCondition(
        new PutIf(new ConditionalExpression(VERSION, toVersionValue(version), Operator.EQ)));

    put.withValues(values);
    mutations.add(put);
  }

  private List<Value> createBeforeValues(Mutation base, TransactionResult result) {
    Key partitionKey = base.getPartitionKey();
    Optional<Key> clusteringKey = getClusteringKey(base, result);

    List<Value> values = new ArrayList<>();
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

  private boolean isBeforeRequired(Value value, Key primary, Optional<Key> clustering) {
    if (!value.getName().startsWith(Attribute.BEFORE_PREFIX)
        && !isValueInKeys(value, primary, clustering)) {
      return true;
    }
    return false;
  }

  private boolean isValueInKeys(Value value, Key primary, Optional<Key> clustering) {
    for (Value v : primary) {
      if (v.equals(value)) {
        return true;
      }
    }

    if (!clustering.isPresent()) {
      return false;
    }

    for (Value v : clustering.get()) {
      if (v.equals(value)) {
        return true;
      }
    }
    return false;
  }
}
