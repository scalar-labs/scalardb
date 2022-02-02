package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import com.scalar.db.storage.common.ResultImpl;
import com.scalar.db.transaction.consensuscommit.ParallelExecutor.ParallelExecutorTask;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class RecoveryHandler {
  @VisibleForTesting static final long TRANSACTION_LIFETIME_MILLIS = 15000;
  private static final Logger LOGGER = LoggerFactory.getLogger(RecoveryHandler.class);

  private final DistributedStorage storage;
  private final Coordinator coordinator;
  private final TransactionalTableMetadataManager tableMetadataManager;
  private final ParallelExecutor parallelExecutor;

  public RecoveryHandler(
      DistributedStorage storage,
      Coordinator coordinator,
      TransactionalTableMetadataManager tableMetadataManager,
      ParallelExecutor parallelExecutor) {
    this.storage = checkNotNull(storage);
    this.coordinator = checkNotNull(coordinator);
    this.tableMetadataManager = checkNotNull(tableMetadataManager);
    this.parallelExecutor = checkNotNull(parallelExecutor);
  }

  // lazy recovery in read phase
  public Optional<TransactionResult> recover(Selection selection, TransactionResult result)
      throws RecoveryException {
    LOGGER.debug("recovering for {}", result.getId());

    // as result doesn't have before image columns, need to get the latest result
    Optional<TransactionResult> latestResult = getLatestResult(selection, result);
    if (!latestResult.isPresent()) {
      // indicates the record is deleted in another transaction
      return Optional.empty();
    }
    if (latestResult.get().isCommitted()) {
      // indicates the record is committed in another transaction
      return latestResult;
    }

    // TODO Execute lazy recovery asynchronously
    Optional<Coordinator.State> state = getState(latestResult.get().getId());
    TransactionalTableMetadata metadata = getTableMetadata(selection);
    if (state.isPresent()) {
      if (state.get().getState().equals(TransactionState.COMMITTED)) {
        rollforwardRecord(selection, latestResult.get());
        return getAfterImageResult(latestResult.get(), metadata);
      } else {
        rollbackRecord(selection, latestResult.get());
        return getBeforeImageResult(latestResult.get(), metadata);
      }
    } else {
      abortIfExpired(selection, latestResult.get());
      return getBeforeImageResult(latestResult.get(), metadata);
    }
  }

  private Optional<TransactionResult> getLatestResult(Selection selection, TransactionResult result)
      throws RecoveryException {
    try {
      Get get =
          new Get(selection.getPartitionKey(), getClusteringKey(selection, result).orElse(null))
              .withConsistency(Consistency.LINEARIZABLE)
              .forNamespace(selection.forNamespace().get())
              .forTable(selection.forTable().get());
      return storage.get(get).map(TransactionResult::new);
    } catch (ExecutionException e) {
      throw new RecoveryException("getting the latest result failed", e);
    }
  }

  private Optional<Key> getClusteringKey(Selection selection, TransactionResult result) {
    if (selection instanceof Scan) {
      return result.getClusteringKey();
    } else {
      return selection.getClusteringKey();
    }
  }

  private Optional<Coordinator.State> getState(String id) throws RecoveryException {
    try {
      return coordinator.getState(id);
    } catch (CoordinatorException e) {
      throw new RecoveryException("getting the coordinator state failed", e);
    }
  }

  private TransactionalTableMetadata getTableMetadata(Selection selection)
      throws RecoveryException {
    try {
      return tableMetadataManager.getTransactionalTableMetadata(selection);
    } catch (ExecutionException e) {
      throw new RecoveryException("getting the table metadata failed", e);
    }
  }

  private Optional<TransactionResult> getAfterImageResult(
      TransactionResult result, TransactionalTableMetadata metadata) {
    if (result.getState().equals(TransactionState.PREPARED)) {
      Map<String, Value<?>> values = new HashMap<>();
      for (String afterImageColumnName : metadata.getAfterImageColumnNames()) {
        assert result.getValue(afterImageColumnName).isPresent();
        if (afterImageColumnName.equals(Attribute.STATE)) {
          // change the state to COMMITTED
          values.put(Attribute.STATE, Attribute.toStateValue(TransactionState.COMMITTED));
        } else {
          values.put(afterImageColumnName, result.getValue(afterImageColumnName).get());
        }
      }
      return Optional.of(
          new TransactionResult(new ResultImpl(values, metadata.getTableMetadata())));
    } else { // TransactionState.DELETED
      return Optional.empty();
    }
  }

  private Optional<TransactionResult> getBeforeImageResult(
      TransactionResult result, TransactionalTableMetadata metadata) {
    assert result.getValue(Attribute.BEFORE_ID).isPresent();
    TextValue beforeId = (TextValue) result.getValue(Attribute.BEFORE_ID).get();
    if (beforeId.get().isPresent()) {
      Map<String, Value<?>> values = new HashMap<>();
      for (String partitionKeyName : metadata.getPartitionKeyNames()) {
        assert result.getValue(partitionKeyName).isPresent();
        values.put(partitionKeyName, result.getValue(partitionKeyName).get());
      }
      for (String clusteringKeyName : metadata.getClusteringKeyNames()) {
        assert result.getValue(clusteringKeyName).isPresent();
        values.put(clusteringKeyName, result.getValue(clusteringKeyName).get());
      }
      for (String beforeImageColumnName : metadata.getBeforeImageColumnNames()) {
        assert result.getValue(beforeImageColumnName).isPresent();
        // convert a before image column to an after image column (by removing the before prefix
        // from the name)
        String convertedName = beforeImageColumnName.substring(Attribute.BEFORE_PREFIX.length());
        values.put(
            convertedName, result.getValue(beforeImageColumnName).get().copyWith(convertedName));
      }
      return Optional.of(
          new TransactionResult(new ResultImpl(values, metadata.getTableMetadata())));
    } else {
      return Optional.empty();
    }
  }

  public void rollbackRecords(Snapshot snapshot) {
    LOGGER.debug("rollback from snapshot for {}", snapshot.getId());
    try {
      RollbackMutationComposer composer = new RollbackMutationComposer(snapshot.getId(), storage);
      snapshot.to(composer);
      PartitionedMutations mutations = new PartitionedMutations(composer.get());

      ImmutableList<PartitionedMutations.Key> orderedKeys = mutations.getOrderedKeys();
      List<ParallelExecutorTask> tasks = new ArrayList<>(orderedKeys.size());
      for (PartitionedMutations.Key key : orderedKeys) {
        tasks.add(() -> storage.mutate(mutations.get(key)));
      }
      parallelExecutor.rollback(tasks);
    } catch (Exception e) {
      LOGGER.warn("rolling back records failed", e);
      // ignore since records are recovered lazily
    }
  }

  @VisibleForTesting
  void rollbackRecord(Selection selection, TransactionResult result) {
    LOGGER.debug(
        "rollback for {}, {} mutated by {}",
        selection.getPartitionKey(),
        selection.getClusteringKey(),
        result.getId());
    RollbackMutationComposer composer = new RollbackMutationComposer(result.getId(), storage);
    composer.add(selection, result);
    mutate(composer.get());
  }

  @VisibleForTesting
  void rollforwardRecord(Selection selection, TransactionResult result) {
    LOGGER.debug(
        "rollforward for {}, {} mutated by {}",
        selection.getPartitionKey(),
        selection.getClusteringKey(),
        result.getId());
    CommitMutationComposer composer = new CommitMutationComposer(result.getId());
    composer.add(selection, result);
    mutate(composer.get());
  }

  private void abortIfExpired(Selection selection, TransactionResult result)
      throws TransactionNotExpiredException {
    long current = System.currentTimeMillis();
    if (current <= result.getPreparedAt() + TRANSACTION_LIFETIME_MILLIS) {
      throw new TransactionNotExpiredException(
          "the transaction " + result.getId() + " is not expired");
    }

    try {
      coordinator.putState(new Coordinator.State(result.getId(), TransactionState.ABORTED));
      rollbackRecord(selection, result);
    } catch (CoordinatorException e) {
      LOGGER.warn("coordinator tries to abort {}, but failed", result.getId(), e);
    }
  }

  private void mutate(List<Mutation> mutations) {
    try {
      storage.mutate(mutations);
    } catch (ExecutionException e) {
      LOGGER.warn("mutation in recovery failed. the record will be eventually recovered", e);
    }
  }
}
