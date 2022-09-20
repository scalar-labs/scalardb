package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.api.ConditionalExpression.Operator;
import static com.scalar.db.transaction.consensuscommit.Attribute.ID;
import static com.scalar.db.transaction.consensuscommit.Attribute.STATE;
import static com.scalar.db.transaction.consensuscommit.Attribute.toIdValue;
import static com.scalar.db.transaction.consensuscommit.Attribute.toStateValue;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class RollbackMutationComposer extends AbstractMutationComposer {

  private final DistributedStorage storage;
  private final TransactionTableMetadataManager tableMetadataManager;

  public RollbackMutationComposer(
      String id, DistributedStorage storage, TransactionTableMetadataManager tableMetadataManager) {
    super(id);
    this.storage = storage;
    this.tableMetadataManager = tableMetadataManager;
  }

  /** rollback in either prepare phase in commit or lazy recovery phase in read */
  @Override
  public void add(Operation base, @Nullable TransactionResult result) throws ExecutionException {
    // We always re-read the latest record here because of the following reasons:
    // 1. for usual rollback, we need to check the latest status of the record
    // 2. for rollback in lazy recovery, the result doesn't have before image columns
    TransactionResult latest = getLatestResult(base, result).orElse(null);
    if (latest == null) {
      // the record was not prepared (yet) by this transaction or has already been rollback deleted
      return;
    }
    if (!latest.getId().equals(id)) {
      // the record was not prepared (yet) by this transaction or has already been rolled back
      return;
    }

    TextValue beforeId = (TextValue) latest.getValue(Attribute.BEFORE_ID).get();
    if (beforeId.get().isPresent()) {
      mutations.add(composePut(base, latest));
    } else {
      // no record to rollback, so it should be deleted
      mutations.add(composeDelete(base, latest));
    }
  }

  private Put composePut(Operation base, TransactionResult result) throws ExecutionException {
    assert result != null
        && (result.getState().equals(TransactionState.PREPARED)
            || result.getState().equals(TransactionState.DELETED));

    TransactionTableMetadata metadata = tableMetadataManager.getTransactionTableMetadata(base);
    LinkedHashSet<String> beforeImageColumnNames = metadata.getBeforeImageColumnNames();

    Map<String, Value<?>> map = new HashMap<>();
    result
        .getValues()
        .forEach(
            (k, v) -> {
              if (beforeImageColumnNames.contains(k)) {
                String key = k.substring(Attribute.BEFORE_PREFIX.length());
                map.put(key, v.copyWith(key));
              }
            });

    return new Put(result.getPartitionKey().get(), result.getClusteringKey().orElse(null))
        .forNamespace(base.forNamespace().get())
        .forTable(base.forTable().get())
        .withCondition(
            new PutIf(
                new ConditionalExpression(ID, toIdValue(id), Operator.EQ),
                new ConditionalExpression(STATE, toStateValue(result.getState()), Operator.EQ)))
        .withConsistency(Consistency.LINEARIZABLE)
        .withValues(map.values());
  }

  private Delete composeDelete(Operation base, TransactionResult result) {
    assert result != null
        && (result.getState().equals(TransactionState.PREPARED)
            || result.getState().equals(TransactionState.DELETED));

    return new Delete(result.getPartitionKey().get(), result.getClusteringKey().orElse(null))
        .forNamespace(base.forNamespace().get())
        .forTable(base.forTable().get())
        .withCondition(
            new DeleteIf(
                new ConditionalExpression(ID, toIdValue(id), Operator.EQ),
                new ConditionalExpression(STATE, toStateValue(result.getState()), Operator.EQ)))
        .withConsistency(Consistency.LINEARIZABLE);
  }

  private Optional<TransactionResult> getLatestResult(
      Operation operation, @Nullable TransactionResult result) throws ExecutionException {
    Key partitionKey;
    Key clusteringKey;
    if (operation instanceof Mutation) {
      // for usual rollback
      partitionKey = operation.getPartitionKey();
      clusteringKey = operation.getClusteringKey().orElse(null);
    } else {
      assert operation instanceof Selection;
      if (result != null) {
        // for rollback in lazy recovery
        partitionKey = result.getPartitionKey().get();
        clusteringKey = result.getClusteringKey().orElse(null);
      } else {
        // for deleting non-existing record that was prepared with DELETED for Serializable with
        // Extra-write
        assert operation instanceof Get;
        partitionKey = operation.getPartitionKey();
        clusteringKey = operation.getClusteringKey().orElse(null);
      }
    }

    Get get =
        new Get(partitionKey, clusteringKey)
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(operation.forNamespace().get())
            .forTable(operation.forTable().get());

    return storage.get(get).map(TransactionResult::new);
  }
}
