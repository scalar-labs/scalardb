package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.api.ConditionalExpression.Operator;
import static com.scalar.db.transaction.consensuscommit.Attribute.ID;
import static com.scalar.db.transaction.consensuscommit.Attribute.STATE;
import static com.scalar.db.transaction.consensuscommit.Attribute.toIdValue;
import static com.scalar.db.transaction.consensuscommit.Attribute.toStateValue;

import com.google.common.annotations.VisibleForTesting;
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
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public class RollbackMutationComposer extends AbstractMutationComposer {
  private static final Logger LOGGER = LoggerFactory.getLogger(RollbackMutationComposer.class);
  private final DistributedStorage storage;
  private final TransactionalTableMetadataManager tableMetadataManager;

  public RollbackMutationComposer(
      String id,
      DistributedStorage storage,
      TransactionalTableMetadataManager tableMetadataManager) {
    super(id);
    this.storage = storage;
    this.tableMetadataManager = tableMetadataManager;
  }

  @VisibleForTesting
  RollbackMutationComposer(
      String id,
      DistributedStorage storage,
      TransactionalTableMetadataManager tableMetadataManager,
      List<Mutation> mutations) {
    super(id, mutations, System.currentTimeMillis());
    this.storage = storage;
    this.tableMetadataManager = tableMetadataManager;
  }

  /** rollback in either prepare phase in commit or lazy recovery phase in read */
  @Override
  public void add(Operation base, TransactionResult result) throws ExecutionException {
    TransactionResult latest;
    if (result == null || !result.getId().equals(id)) {
      // rollback from snapshot
      latest = getLatestResult(base, result).orElse(null);
      if (latest == null) {
        LOGGER.debug("the record was not prepared or has already rollback deleted");
        return;
      }
      if (!latest.getId().equals(id)) {
        LOGGER.debug(
            "the record is not prepared (yet) by this transaction or has already rolled back");
        return;
      }
    } else {
      latest = result;
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
    assert result.getState().equals(TransactionState.PREPARED)
        || result.getState().equals(TransactionState.DELETED);

    TransactionalTableMetadata metadata = tableMetadataManager.getTransactionalTableMetadata(base);
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

    return new Put(base.getPartitionKey(), getClusteringKey(base, result).orElse(null))
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
    assert result.getState().equals(TransactionState.PREPARED)
        || result.getState().equals(TransactionState.DELETED);

    return new Delete(base.getPartitionKey(), getClusteringKey(base, result).orElse(null))
        .forNamespace(base.forNamespace().get())
        .forTable(base.forTable().get())
        .withCondition(
            new DeleteIf(
                new ConditionalExpression(ID, toIdValue(id), Operator.EQ),
                new ConditionalExpression(STATE, toStateValue(result.getState()), Operator.EQ)))
        .withConsistency(Consistency.LINEARIZABLE);
  }

  private Optional<TransactionResult> getLatestResult(Operation operation, TransactionResult result)
      throws ExecutionException {
    Get get =
        new Get(operation.getPartitionKey(), getClusteringKey(operation, result).orElse(null))
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(operation.forNamespace().get())
            .forTable(operation.forTable().get());

    return storage.get(get).map(TransactionResult::new);
  }
}
