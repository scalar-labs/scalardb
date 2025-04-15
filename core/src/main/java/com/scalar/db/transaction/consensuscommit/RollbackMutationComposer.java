package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.api.ConditionalExpression.Operator;
import static com.scalar.db.transaction.consensuscommit.Attribute.ID;
import static com.scalar.db.transaction.consensuscommit.Attribute.STATE;
import static com.scalar.db.transaction.consensuscommit.Attribute.toIdValue;
import static com.scalar.db.transaction.consensuscommit.Attribute.toStateValue;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Column;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class RollbackMutationComposer extends AbstractMutationComposer {

  private final DistributedStorage storage;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public RollbackMutationComposer(
      String id, DistributedStorage storage, TransactionTableMetadataManager tableMetadataManager) {
    super(id, tableMetadataManager);
    this.storage = storage;
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
    if (!Objects.equals(latest.getId(), id)) {
      // This is the case for the record that was not prepared (yet) by this transaction or has
      // already been rolled back. We need to use Objects.equals() here since the transaction ID of
      // the latest record can be NULL (and different from this transaction's ID) when the record
      // has already been rolled back to the deemed committed state by another transaction.
      return;
    }

    if (latest.hasBeforeImage()) {
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

    List<Column<?>> columns = new ArrayList<>();
    result
        .getColumns()
        .forEach(
            (k, v) -> {
              if (beforeImageColumnNames.contains(k)) {
                String key = k.substring(Attribute.BEFORE_PREFIX.length());
                if (key.equals(Attribute.VERSION) && v.getIntValue() == 0) {
                  // Since we use version 0 instead of copying NULL for before_version when updating
                  // a NULL-transaction-metadata record, we conversely change 0 to NULL for
                  // rollback. See also PrepareMutationComposer.
                  columns.add(IntColumn.ofNull(Attribute.VERSION));
                } else {
                  columns.add(v.copyWith(key));
                }
              }
            });

    Key partitionKey = ScalarDbUtils.getPartitionKey(result, metadata.getTableMetadata());
    Optional<Key> clusteringKey =
        ScalarDbUtils.getClusteringKey(result, metadata.getTableMetadata());

    PutBuilder.Buildable putBuilder =
        Put.newBuilder()
            .namespace(base.forNamespace().get())
            .table(base.forTable().get())
            .partitionKey(partitionKey)
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column(ID).isEqualToText(id))
                    .and(ConditionBuilder.column(STATE).isEqualToInt(result.getState().get()))
                    .build())
            .consistency(Consistency.LINEARIZABLE);
    clusteringKey.ifPresent(putBuilder::clusteringKey);
    columns.forEach(putBuilder::value);

    return putBuilder.build();
  }

  private Delete composeDelete(Operation base, TransactionResult result) throws ExecutionException {
    assert result != null
        && (result.getState().equals(TransactionState.PREPARED)
            || result.getState().equals(TransactionState.DELETED));

    TransactionTableMetadata metadata = tableMetadataManager.getTransactionTableMetadata(base);
    Key partitionKey = ScalarDbUtils.getPartitionKey(result, metadata.getTableMetadata());
    Optional<Key> clusteringKey =
        ScalarDbUtils.getClusteringKey(result, metadata.getTableMetadata());

    return new Delete(partitionKey, clusteringKey.orElse(null))
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
    @Nullable Key clusteringKey;
    if (operation instanceof Mutation) {
      // for usual rollback
      partitionKey = operation.getPartitionKey();
      clusteringKey = operation.getClusteringKey().orElse(null);
    } else {
      assert operation instanceof Selection;
      if (result != null) {
        // for rollback in lazy recovery
        TransactionTableMetadata metadata =
            tableMetadataManager.getTransactionTableMetadata(operation);
        partitionKey = ScalarDbUtils.getPartitionKey(result, metadata.getTableMetadata());
        clusteringKey =
            ScalarDbUtils.getClusteringKey(result, metadata.getTableMetadata()).orElse(null);
      } else {
        throw new AssertionError(
            "This path should not be reached since the EXTRA_WRITE strategy is deleted");
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
