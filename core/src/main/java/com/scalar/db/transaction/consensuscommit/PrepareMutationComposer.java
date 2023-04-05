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
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.Value;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class PrepareMutationComposer extends AbstractMutationComposer {

  private final TransactionTableMetadataManager tableMetadataManager;

  public PrepareMutationComposer(String id, TransactionTableMetadataManager tableMetadataManager) {
    super(id);
    this.tableMetadataManager = tableMetadataManager;
  }

  @VisibleForTesting
  PrepareMutationComposer(
      String id, long current, TransactionTableMetadataManager tableMetadataManager) {
    super(id, current);
    this.tableMetadataManager = tableMetadataManager;
  }

  @Override
  public void add(Operation base, @Nullable TransactionResult result) throws ExecutionException {
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

  private void add(Put base, @Nullable TransactionResult result) throws ExecutionException {
    Put put =
        new Put(base.getPartitionKey(), base.getClusteringKey().orElse(null))
            .forNamespace(base.forNamespace().get())
            .forTable(base.forTable().get())
            .withConsistency(Consistency.LINEARIZABLE);

    put.withValue(Attribute.toIdValue(id));
    put.withValue(Attribute.toStateValue(TransactionState.PREPARED));
    put.withValue(Attribute.toPreparedAtValue(current));
    base.getColumns().values().forEach(put::withValue);

    if (result != null) { // overwrite existing record
      put.withValues(createBeforeValues(base, result));
      int version = result.getVersion();
      put.withValue(Attribute.toVersionValue(version + 1));

      List<ConditionalExpression> conditions = new ArrayList<>();
      // check if the record is not interrupted by other conflicting transactions
      conditions.add(new ConditionalExpression(VERSION, toVersionValue(version), Operator.EQ));
      conditions.add(new ConditionalExpression(ID, toIdValue(result.getId()), Operator.EQ));
      // add the base operation conditions
      if (base.getCondition().isPresent()) {
        MutationCondition condition = base.getCondition().get();
        if (condition instanceof PutIf) {
          conditions.addAll(base.getCondition().get().getExpressions());
        } else if ((condition instanceof PutIfNotExists)) {
          throw new NoMutationException("the record exist so the condition is not satisfied.");
        }
        // Do nothing if the condition is a PutIfExists since the PutIf condition set below
        // ensure the record exists
      }
      put.withCondition(new PutIf(conditions));
    } else { // initial record
      put.withValue(Attribute.toVersionValue(1));

      if (base.getCondition().isPresent()) {
        MutationCondition condition = base.getCondition().get();
        if (condition instanceof PutIf || condition instanceof PutIfExists) {
          throw new NoMutationException(
              "the record does not exist so the condition is not satisfied.");
        }
      }
      // check if the record is not created by other conflicting transactions
      put.withCondition(new PutIfNotExists());
    }

    mutations.add(put);
  }

  private void add(Delete base, @Nullable TransactionResult result) throws ExecutionException {
    Put put =
        new Put(base.getPartitionKey(), base.getClusteringKey().orElse(null))
            .forNamespace(base.forNamespace().get())
            .forTable(base.forTable().get())
            .withConsistency(Consistency.LINEARIZABLE);

    List<Value<?>> values = new ArrayList<>();
    values.add(Attribute.toIdValue(id));
    values.add(Attribute.toStateValue(TransactionState.DELETED));
    values.add(Attribute.toPreparedAtValue(current));

    if (result != null) {
      values.addAll(createBeforeValues(base, result));
      int version = result.getVersion();
      values.add(Attribute.toVersionValue(version + 1));

      List<ConditionalExpression> conditions = new ArrayList<>();
      // check if the record is not interrupted by other conflicting transactions
      conditions.add(new ConditionalExpression(VERSION, toVersionValue(version), Operator.EQ));
      conditions.add(new ConditionalExpression(ID, toIdValue(result.getId()), Operator.EQ));

      // add the base operation conditions
      if (base.getCondition().isPresent()) {
        if (base.getCondition().get() instanceof DeleteIf) {
          conditions.addAll(base.getCondition().get().getExpressions());
        }
        // Do nothing when the condition is a DeleteIfExists since the PutIf condition set below
        // ensure the record exists
      }

      put.withCondition(new PutIf(conditions));
    } else {
      put.withValue(Attribute.toVersionValue(1));

      if (base.getCondition().isPresent()) {
        // DeleteIf or DeleteIfExists
        throw new NoMutationException(
            "the record does not exist so the condition is not satisfied.");
      }

      // check if the record is not created by other conflicting transactions
      put.withCondition(new PutIfNotExists());
    }

    put.withValues(values);
    mutations.add(put);
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

  private List<Value<?>> createBeforeValues(Mutation base, TransactionResult result)
      throws ExecutionException {
    List<Value<?>> values = new ArrayList<>();
    for (Value<?> value : result.getValues().values()) {
      if (isBeforeRequired(base, value.getName())) {
        values.add(value.copyWith(Attribute.BEFORE_PREFIX + value.getName()));
      }
    }
    return values;
  }

  private boolean isBeforeRequired(Mutation base, String columnName) throws ExecutionException {
    TransactionTableMetadata metadata = tableMetadataManager.getTransactionTableMetadata(base);
    return !metadata.getPrimaryKeyColumnNames().contains(columnName)
        && metadata.getAfterImageColumnNames().contains(columnName);
  }
}
