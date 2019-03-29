package com.scalar.database.transaction.consensuscommit;

import static com.scalar.database.api.ConditionalExpression.Operator;
import static com.scalar.database.transaction.consensuscommit.Attribute.*;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.database.api.ConditionalExpression;
import com.scalar.database.api.Consistency;
import com.scalar.database.api.Delete;
import com.scalar.database.api.DeleteIf;
import com.scalar.database.api.DistributedStorage;
import com.scalar.database.api.Get;
import com.scalar.database.api.Mutation;
import com.scalar.database.api.Operation;
import com.scalar.database.api.Put;
import com.scalar.database.api.PutIf;
import com.scalar.database.api.TransactionState;
import com.scalar.database.exception.storage.ExecutionException;
import com.scalar.database.exception.transaction.TransactionRuntimeException;
import com.scalar.database.io.Key;
import com.scalar.database.io.TextValue;
import com.scalar.database.io.Value;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prepare mutations from a given {@link Snapshot} in order to rollback {@link DistributedStorage}.
 */
@NotThreadSafe
public class RollbackMutationComposer extends AbstractMutationComposer {
  private static final Logger LOGGER = LoggerFactory.getLogger(RollbackMutationComposer.class);
  private final DistributedStorage storage;

  public RollbackMutationComposer(String id, DistributedStorage storage) {
    super(id);
    this.storage = storage;
  }

  @VisibleForTesting
  RollbackMutationComposer(String id, DistributedStorage storage, List<Mutation> mutations) {
    super(id, mutations, System.currentTimeMillis());
    this.storage = storage;
  }

  /** rollback in either prepare phase in commit or lazy recovery phase in read */
  @Override
  public void add(Operation base, TransactionResult result) {
    TransactionResult latest;
    if (result == null || !result.getId().equals(id)) {
      // rollback from snapshot
      try {
        latest = getLatestResult(base, result).orElse(null);
      } catch (ExecutionException e) {
        LOGGER.warn(e.getMessage());
        return;
      }
      if (latest == null) {
        LOGGER.info("the record was not prepared or has already rollback deleted");
        return;
      }

      if (!latest.getId().equals(id)) {
        LOGGER.info(
            "the record is not prepared (yet) by this transaction " + "or has already rollbacked");
        return;
      }
    } else {
      latest = result;
    }

    TextValue beforeId = (TextValue) latest.getValue(Attribute.BEFORE_ID).get();
    if (beforeId.getString().isPresent()) {
      mutations.add(composePut(base, latest));
    } else {
      // no record to rollback, so it should be deleted
      mutations.add(composeDelete(base, latest));
    }
  }

  private Put composePut(Operation base, TransactionResult result) {
    if (!result.getState().equals(TransactionState.PREPARED)
        && !result.getState().equals(TransactionState.DELETED)) {
      throw new TransactionRuntimeException("rollback is toward non-prepared record");
    }
    Map<String, Value> map = new HashMap<>();
    result
        .getValues()
        .forEach(
            (k, v) -> {
              if (k.startsWith(Attribute.BEFORE_PREFIX)) {
                String key = k.substring(Attribute.BEFORE_PREFIX.length());
                map.put(key, v.copyWith(key));
              }
            });

    // remove keys
    Stream.of(
            base.getPartitionKey().get(),
            getClusteringKey(base, result).map(Key::get).orElse(Collections.emptyList()))
        .flatMap(Collection::stream)
        .forEach(v -> map.remove(v.getName()));

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
    if (!result.getState().equals(TransactionState.PREPARED)) {
      throw new TransactionRuntimeException("rollback is toward non-prepared record");
    }
    return new Delete(base.getPartitionKey(), getClusteringKey(base, result).orElse(null))
        .forNamespace(base.forNamespace().get())
        .forTable(base.forTable().get())
        .withCondition(
            new DeleteIf(
                new ConditionalExpression(ID, toIdValue(id), Operator.EQ),
                // the state should be PREPARED
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
